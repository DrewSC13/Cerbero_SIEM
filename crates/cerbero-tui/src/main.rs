#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::struct_excessive_bools)]

use std::env;
use std::error::Error;
use std::io::{self, stdout};
use std::time::Duration as StdDuration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc;

const TENANT_HEADER: &str = "X-Cerbero-Tenant-ID";
const VIEWS: [&str; 11] = [
    "Overview",
    "Events",
    "Search",
    "Findings",
    "Incidents",
    "Cases",
    "Entities",
    "ATT&CK",
    "Rules",
    "Agents",
    "System",
];

#[derive(Clone, Debug, Serialize)]
struct SearchTimeRange {
    from: String,
    to: String,
}

#[derive(Clone, Debug, Serialize)]
struct SearchSort {
    field: String,
    direction: String,
}

#[derive(Clone, Debug, Serialize)]
struct SearchRequest {
    query: String,
    time_range: SearchTimeRange,
    #[serde(skip_serializing_if = "String::is_empty")]
    cursor: String,
    limit: usize,
    sort: Vec<SearchSort>,
}

#[derive(Clone, Debug, Deserialize)]
struct SearchExecution {
    request_id: String,
    partial_result: bool,
    truncated: bool,
    timed_out: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct SearchResponse {
    items: Vec<EventItem>,
    next_cursor: String,
    execution: SearchExecution,
}

#[derive(Clone, Debug, Deserialize)]
struct EventItem {
    normalized_event_id: String,
    tenant_id: String,
    #[serde(default)]
    event_time: String,
    #[serde(default)]
    severity_id: Option<u32>,
    ocsf_event: Value,
}

#[derive(Clone)]
struct ApiClient {
    http: Client,
    base_url: String,
    tenant_id: String,
}

impl ApiClient {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let base_url = env::var("CERBERO_TUI_API_URL")?;
        let tenant_id = env::var("CERBERO_TUI_TENANT_ID")?;
        if !(base_url.starts_with("http://127.0.0.1:") || base_url.starts_with("http://localhost:"))
        {
            return Err("Step 32 DEVELOPMENT TUI requires a loopback CERBERO_TUI_API_URL".into());
        }
        Ok(Self {
            http: Client::builder()
                .timeout(StdDuration::from_secs(6))
                .build()?,
            base_url: base_url.trim_end_matches('/').to_owned(),
            tenant_id,
        })
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, String> {
        let response = self
            .http
            .post(format!("{}/api/v1/search", self.base_url))
            .header(TENANT_HEADER, &self.tenant_id)
            .json(request)
            .send()
            .await
            .map_err(|error| format!("API transport error: {error}"))?;
        let status = response.status();
        let request_id = response
            .headers()
            .get("X-Request-ID")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read API error".to_owned());
            return Err(format!(
                "API {}: {} (request {})",
                status.as_u16(),
                body.trim(),
                request_id
            ));
        }
        response
            .json::<SearchResponse>()
            .await
            .map_err(|error| format!("API response decode error: {error}"))
    }
}

#[derive(Clone, Debug)]
struct Cli {
    snapshot: bool,
    query: String,
    from: Option<String>,
    to: Option<String>,
    limit: usize,
}

impl Cli {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut snapshot = false;
        let mut query = String::new();
        let mut from = None;
        let mut to = None;
        let mut limit = 50_usize;
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--snapshot" => snapshot = true,
                "--query" => query = arguments.next().ok_or("--query requires a value")?,
                "--from" => from = Some(arguments.next().ok_or("--from requires a value")?),
                "--to" => to = Some(arguments.next().ok_or("--to requires a value")?),
                "--limit" => {
                    limit = arguments
                        .next()
                        .ok_or("--limit requires a value")?
                        .parse()?;
                }
                "--help" | "-h" => {
                    println!(
                        "cerbero-tui [--snapshot] [--query QUERY] [--from RFC3339 --to RFC3339] [--limit N]"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}").into()),
            }
        }
        if from.is_some() != to.is_some() {
            return Err("--from and --to must be supplied together".into());
        }
        if !(1..=200).contains(&limit) {
            return Err("--limit must be 1..200".into());
        }
        Ok(Self {
            snapshot,
            query,
            from,
            to,
            limit,
        })
    }
}

#[derive(Clone, Debug)]
struct App {
    tenant_id: String,
    view_index: usize,
    query: String,
    editing: bool,
    loading: bool,
    items: Vec<EventItem>,
    next_cursor: String,
    status: String,
    request_id: String,
    partial: bool,
    from: String,
    to: String,
    limit: usize,
}

impl App {
    fn new(tenant_id: String, cli: &Cli) -> Result<Self, Box<dyn Error>> {
        let (from, to) = match (&cli.from, &cli.to) {
            (Some(from), Some(to)) => (from.clone(), to.clone()),
            _ => default_time_range()?,
        };
        Ok(Self {
            tenant_id,
            view_index: 2,
            query: cli.query.clone(),
            editing: false,
            loading: false,
            items: Vec::new(),
            next_cursor: String::new(),
            status: "ready".to_owned(),
            request_id: String::new(),
            partial: false,
            from,
            to,
            limit: cli.limit,
        })
    }

    fn request(&self, cursor: String) -> SearchRequest {
        SearchRequest {
            query: self.query.clone(),
            time_range: SearchTimeRange {
                from: self.from.clone(),
                to: self.to.clone(),
            },
            cursor,
            limit: self.limit,
            sort: vec![SearchSort {
                field: "event_time".to_owned(),
                direction: "desc".to_owned(),
            }],
        }
    }

    fn apply_search(&mut self, response: SearchResponse) {
        self.items = response.items;
        self.next_cursor = response.next_cursor;
        self.request_id = response.execution.request_id;
        self.partial = response.execution.partial_result
            || response.execution.truncated
            || response.execution.timed_out;
        self.loading = false;
        self.status = format!("{} result(s)", self.items.len());
    }

    fn apply_error(&mut self, error: String) {
        self.loading = false;
        self.status = error;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Quit,
    NextView,
    PreviousView,
    BeginSearchEdit,
    CommitSearch,
    CancelEdit,
    Backspace,
    Insert(char),
    Refresh,
    NextPage,
    None,
}

fn keymap(key: KeyEvent, editing: bool) -> Action {
    if editing {
        return match key.code {
            KeyCode::Esc => Action::CancelEdit,
            KeyCode::Enter => Action::CommitSearch,
            KeyCode::Backspace => Action::Backspace,
            KeyCode::Char(character) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                Action::Insert(character)
            }
            _ => Action::None,
        };
    }
    match key.code {
        KeyCode::Char('q') => Action::Quit,
        KeyCode::Tab | KeyCode::Down => Action::NextView,
        KeyCode::BackTab | KeyCode::Up => Action::PreviousView,
        KeyCode::Char('/') => Action::BeginSearchEdit,
        KeyCode::Char('r') => Action::Refresh,
        KeyCode::Char('n') => Action::NextPage,
        _ => Action::None,
    }
}

enum ApiCommand {
    Search(SearchRequest),
}

async fn api_worker(
    client: ApiClient,
    mut commands: mpsc::UnboundedReceiver<ApiCommand>,
    results: mpsc::UnboundedSender<Result<SearchResponse, String>>,
) {
    while let Some(ApiCommand::Search(request)) = commands.recv().await {
        let result = client.search(&request).await;
        let _ = results.send(result);
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        execute!(stdout(), EnterAlternateScreen)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(stdout(), LeaveAlternateScreen);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse()?;
    let client = ApiClient::from_env()?;
    if cli.snapshot {
        return snapshot(client, &cli).await;
    }
    run_interactive(client, &cli)
}

async fn snapshot(client: ApiClient, cli: &Cli) -> Result<(), Box<dyn Error>> {
    let app = App::new(client.tenant_id.clone(), cli)?;
    let response = client
        .search(&app.request(String::new()))
        .await
        .map_err(io::Error::other)?;
    println!("CERBERO Search snapshot tenant={}", client.tenant_id);
    println!("query={}", cli.query);
    println!("request_id={}", response.execution.request_id);
    println!("partial={}", response.execution.partial_result);
    for item in response.items {
        println!(
            "{} | tenant={} | {} | user={} | {}",
            item.normalized_event_id,
            item.tenant_id,
            item.event_time,
            event_user(&item),
            event_message(&item)
        );
    }
    Ok(())
}

fn run_interactive(client: ApiClient, cli: &Cli) -> Result<(), Box<dyn Error>> {
    let _guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    let mut app = App::new(client.tenant_id.clone(), cli)?;
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    tokio::spawn(api_worker(client, command_rx, result_tx));

    command_tx
        .send(ApiCommand::Search(app.request(String::new())))
        .map_err(|_| io::Error::other("API worker stopped"))?;
    app.loading = true;
    "loading search".clone_into(&mut app.status);

    loop {
        terminal.draw(|frame| render(frame, &app))?;
        while let Ok(result) = result_rx.try_recv() {
            match result {
                Ok(response) => app.apply_search(response),
                Err(error) => app.apply_error(error),
            }
        }
        if !event::poll(StdDuration::from_millis(50))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        match keymap(key, app.editing) {
            Action::Quit => break,
            Action::NextView => app.view_index = (app.view_index + 1) % VIEWS.len(),
            Action::PreviousView => {
                app.view_index = (app.view_index + VIEWS.len() - 1) % VIEWS.len();
            }
            Action::BeginSearchEdit => {
                app.view_index = 2;
                app.editing = true;
                "editing query".clone_into(&mut app.status);
            }
            Action::CommitSearch => {
                app.editing = false;
                app.loading = true;
                app.next_cursor.clear();
                "loading search".clone_into(&mut app.status);
                command_tx
                    .send(ApiCommand::Search(app.request(String::new())))
                    .map_err(|_| io::Error::other("API worker stopped"))?;
            }
            Action::CancelEdit => {
                app.editing = false;
                "query edit cancelled".clone_into(&mut app.status);
            }
            Action::Backspace => {
                app.query.pop();
            }
            Action::Insert(character) => app.query.push(character),
            Action::Refresh => {
                app.loading = true;
                "refreshing".clone_into(&mut app.status);
                command_tx
                    .send(ApiCommand::Search(app.request(String::new())))
                    .map_err(|_| io::Error::other("API worker stopped"))?;
            }
            Action::NextPage => {
                if !app.next_cursor.is_empty() {
                    app.loading = true;
                    "loading next page".clone_into(&mut app.status);
                    command_tx
                        .send(ApiCommand::Search(app.request(app.next_cursor.clone())))
                        .map_err(|_| io::Error::other("API worker stopped"))?;
                }
            }
            Action::None => {}
        }
    }
    terminal.show_cursor()?;
    Ok(())
}

fn render(frame: &mut Frame<'_>, app: &App) {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(frame.area());
    let header = Paragraph::new(Line::from(vec![
        Span::styled("CERBERO", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            "  tenant={}  view={}  {}",
            app.tenant_id,
            VIEWS[app.view_index],
            if app.loading { "LOADING" } else { "READY" }
        )),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, vertical[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(16), Constraint::Min(40)])
        .split(vertical[1]);
    render_navigation(frame, body[0], app);
    render_workspace(frame, body[1], app);

    let footer_text = format!(
        "{}  request={}  / edit  r refresh  n next  Tab views  q quit{}",
        app.status,
        if app.request_id.is_empty() {
            "-"
        } else {
            &app.request_id
        },
        if app.partial { "  PARTIAL" } else { "" }
    );
    frame.render_widget(
        Paragraph::new(footer_text).block(Block::default().borders(Borders::ALL)),
        vertical[2],
    );
}

fn render_navigation(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let items = VIEWS.iter().enumerate().map(|(index, view)| {
        let line = if index == app.view_index {
            Line::from(Span::styled(
                format!("> {view}"),
                Style::default().add_modifier(Modifier::BOLD),
            ))
        } else {
            Line::from(format!("  {view}"))
        };
        ListItem::new(line)
    });
    frame.render_widget(
        List::new(items).block(Block::default().title("Views").borders(Borders::ALL)),
        area,
    );
}

fn render_workspace(frame: &mut Frame<'_>, area: Rect, app: &App) {
    match VIEWS[app.view_index] {
        "Search" | "Events" => render_events(frame, area, app),
        view => {
            let text = format!(
                "{view}\n\nThis canonical view is reserved in the v1 navigation.\nStep 32 first vertical makes Search/Events operational through the API."
            );
            frame.render_widget(
                Paragraph::new(text)
                    .wrap(Wrap { trim: true })
                    .block(Block::default().title(view).borders(Borders::ALL)),
                area,
            );
        }
    }
}

fn render_events(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(4)])
        .split(area);
    let query_title = if app.editing {
        "Query [editing]"
    } else {
        "Query"
    };
    frame.render_widget(
        Paragraph::new(app.query.as_str())
            .block(Block::default().title(query_title).borders(Borders::ALL)),
        chunks[0],
    );
    let rows = app.items.iter().map(|item| {
        ListItem::new(format!(
            "{}  sev={}  user={}  {}  {}",
            item.event_time,
            item.severity_id
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
            event_user(item),
            event_message(item),
            item.normalized_event_id
        ))
    });
    frame.render_widget(
        List::new(rows).block(
            Block::default()
                .title("NormalizedEvents")
                .borders(Borders::ALL),
        ),
        chunks[1],
    );
}

fn event_user(item: &EventItem) -> String {
    item.ocsf_event
        .pointer("/user/name")
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_owned()
}

fn event_message(item: &EventItem) -> String {
    item.ocsf_event
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_owned()
}

fn default_time_range() -> Result<(String, String), Box<dyn Error>> {
    let to = OffsetDateTime::now_utc();
    let from = to - Duration::minutes(15);
    Ok((from.format(&Rfc3339)?, to.format(&Rfc3339)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn keymap_keeps_essential_workflow_keyboard_only() {
        assert_eq!(keymap(key(KeyCode::Tab), false), Action::NextView);
        assert_eq!(
            keymap(key(KeyCode::Char('/')), false),
            Action::BeginSearchEdit
        );
        assert_eq!(keymap(key(KeyCode::Enter), true), Action::CommitSearch);
        assert_eq!(keymap(key(KeyCode::Char('q')), false), Action::Quit);
    }

    #[test]
    fn canonical_navigation_contains_locked_primary_views() {
        assert_eq!(
            VIEWS,
            [
                "Overview",
                "Events",
                "Search",
                "Findings",
                "Incidents",
                "Cases",
                "Entities",
                "ATT&CK",
                "Rules",
                "Agents",
                "System"
            ]
        );
    }

    #[test]
    fn search_view_renders_on_test_backend() {
        let cli = Cli {
            snapshot: false,
            query: "user.name == \"jdoe\"".to_owned(),
            from: Some("2033-05-18T03:33:00Z".to_owned()),
            to: Some("2033-05-18T03:34:00Z".to_owned()),
            limit: 50,
        };
        let mut app =
            App::new("018f47a2-4b00-7a00-8000-00000000f001".to_owned(), &cli).expect("app");
        app.items.push(EventItem {
            normalized_event_id: "018f47a2-4b00-7a00-8000-00000000f101".to_owned(),
            tenant_id: app.tenant_id.clone(),
            event_time: "2033-05-18T03:33:20Z".to_owned(),
            severity_id: Some(2),
            ocsf_event: serde_json::json!({"user":{"name":"jdoe"},"message":"test"}),
        });
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal.draw(|frame| render(frame, &app)).expect("draw");
    }
}
