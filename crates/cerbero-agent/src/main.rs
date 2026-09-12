#![forbid(unsafe_code)]

fn main() {
    println!(
        "{} cerbero-agent bootstrap (architecture {})",
        cerbero_common::PROJECT_NAME,
        cerbero_common::repository_baseline()
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn common_baseline_is_available() {
        assert_eq!(cerbero_common::repository_baseline(), "v1.0");
    }
}
