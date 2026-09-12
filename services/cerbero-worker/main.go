package main

import "fmt"

const componentName = "cerbero-worker"
const architectureBaseline = "v1.0"

func main() {
	fmt.Printf("CERBERO %s bootstrap (architecture %s)\n", componentName, architectureBaseline)
}
