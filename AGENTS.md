# Field Engineering Public Repository

This repository is a collection of independent tools and examples maintained by
different people. Treat each tool directory as its own project. Do not install,
build, lint, or test unrelated directories.

## Working in an existing tool directory

1. Read the nearest `AGENTS.md`, `README.md`, and package manifests.
2. If the directory has a `mise.toml`, run the following from that directory:

   ```bash
   mise trust
   mise install
   mise run setup
   ```

   `mise install` installs only the tools declared by that project. The `setup`
   task installs only that project's dependencies. Use `mise tasks` to discover
   its check, test, and build tasks.
3. If there is no `mise.toml`, do not fall back to a repository-wide install.
   Documentation-only and standard-library scripts may need no setup. Add local
   configuration only when the task genuinely requires it.
4. Run checks from the project directory. Do not assume commands documented for
   one sibling project apply to another.

`.agents/setup` intentionally installs only `mise`, the project-local tool
dispatcher. It must not install a root toolchain or dependencies for every tool.

## Creating a new tool directory

- Make the directory self-contained. Put its source, manifest, lockfile, tests,
  and usage documentation together.
- Add a local `mise.toml` when the tool needs a language runtime, package manager,
  compiler, or external CLI. Pin compatible versions rather than using `latest`.
- Define a `setup` task and the relevant `check`, `test`, or `build` tasks. Tasks
  must be non-interactive and safe to rerun.
- Commit dependency lockfiles. Do not commit virtual environments, downloaded
  dependencies, credentials, generated logs, or editor state.
- Do not add tools to the repository root. A new tool must not make unrelated
  tools slower or change their development environment.
- Add a local `AGENTS.md` only for workflow or maintenance rules that are unique
  to that tool.

Minimal Python example:

```toml
[tools]
python = "3.13"
uv = "0.12.5"

[tasks.setup]
run = "uv sync --frozen"

[tasks.test]
run = "uv run python -m unittest discover tests"
```

Minimal Node example:

```toml
[tools]
node = "22"

[tasks.setup]
run = "npm ci"

[tasks.test]
run = "npm test"
```

## General guidelines

- Follow the patterns and instructions in the tool directory being changed.
- Keep changes scoped to that tool unless the request explicitly spans projects.
- All committed content must meet PUBLIC data-classification requirements. Never
  commit customer data, access tokens, credentials, or private endpoints.
- For TypeScript, enable strict checking, prefer modern ES modules and syntax,
  use interfaces for object contracts, and follow HTTP status-code conventions
  in APIs.
- Use constructor injection and immutable dependencies in Spring applications.
- Use the Pet Store API for public API examples: <https://petstore3.swagger.io>.
