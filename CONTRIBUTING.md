# Contributing to State Actor

Thank you for your interest in contributing to State Actor!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/state-actor.git`
3. Create a branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `go test -v ./...`
6. Commit and push
7. Open a Pull Request

## Development Setup

```bash
# Clone
git clone https://github.com/nerolation/state-actor.git
cd state-actor

# Build
go build -o state-actor .

# Test
go test -v ./...

# Test with race detector
go test -race ./...
```

## Code Style

- Follow standard Go conventions
- Run `go fmt` before committing
- Add tests for new functionality
- Document exported functions

## Testing

All changes should include tests:

```bash
# Run all tests
go test -v ./...

# Run specific package tests
go test -v ./generator

# Run benchmarks
go test -bench=. ./generator
```

## Pull Request Guidelines

1. **Keep PRs focused** — one feature or fix per PR
2. **Write clear commit messages** — explain what and why
3. **Add tests** — especially for new features
4. **Update docs** — if your change affects usage
5. **Pass CI** — all tests must pass

## Reporting Issues

When reporting issues, please include:

- State Actor version
- Go version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

## Feature Requests

Feature requests are welcome! Please:

1. Check existing issues first
2. Describe the use case
3. Explain the expected behavior
4. Consider if you'd like to implement it

## Questions

For questions, you can:

- Open a GitHub Discussion
- Check existing issues/discussions
- Read the documentation in `/docs`

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
