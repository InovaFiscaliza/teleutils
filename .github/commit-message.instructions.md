Generate concise, meaningful commit messages in English following Conventional Commits specification.

📋 Format: <type>[optional scope]: <emoji> <description>
[optional body, always in bullet points if present]
[optional footer(s), always in bullet points if present]

🏷️ Types with emojis:
- feat: ✨ new feature for users
- fix: 🐛 bug fix for users
- docs: 📚 documentation changes
- style: 💄 formatting, missing semi colons, etc (no code change)
- refactor: ♻️ code change that neither fixes bug nor adds feature
- perf: ⚡ code change that improves performance
- test: 🧪 adding missing tests or correcting existing tests
- build: 🔧 changes affecting build system or external dependencies
- ci: 👷 changes to CI configuration files and scripts
- chore: 🔨 other changes that don't modify src or test files
- revert: ⏪ reverts a previous commit
- security: 🔒 security improvements
- deps: 📦 dependency updates

📏 Rules:
- Use imperative mood (\"add\", \"fix\", \"update\", not \"added\", \"fixed\", \"updated\")
- Subject line ≤50 characters, no trailing period
- Body lines ≤72 characters when needed
- Capitalize first letter of description after emoji
- Use scope for context (component, file, module)
- Add breaking change footer: \"BREAKING CHANGE: <description>\"
- Reference issues: \"Closes #123\", \"Fixes #456\"
- Always include relevant emoji after type

💡 Examples:
- feat(auth): ✨ Add OAuth2 Google integration
- fix(api): 🐛 Handle null response in user endpoint
- docs: 📚 Update README with new installation steps
- refactor(utils): ♻️ Extract date formatting to separate module
- test(auth): 🧪 Add integration tests for login flow
- perf(db): ⚡ Optimize user query with proper indexing
- ci: 👷 Add automated security scanning workflow
- build(deps): 🔧 Bump typescript from 4.9.5 to 5.0.2
- security: 🔒 Implement input validation for user forms
- chore(deps): 📦 Update all dependencies to latest versions

🚨 For breaking changes:
feat(api)!: ✨ Remove deprecated v1 endpoints

BREAKING CHANGE: v1 API endpoints have been removed. Use v2 endpoints instead.

🔍 Analyzing staged changes:
- Base the message only on the staged changes. Ignore unstaged and untracked files.
- Read the staged diff itself, not only the file names, and infer what changed from the added and removed lines.
- Classify every changed file or hunk before choosing the type:
  - New code: new files, functions, classes, methods, parameters, branches or columns (mostly added lines with executable statements) → feat.
  - Changed existing code: executable lines both removed and added, such as conditions, expressions, signatures or return values → fix if it corrects wrong behavior, perf if it speeds up, refactor if behavior is unchanged.
  - Documentation only: changes limited to docstrings, comments or markdown, even when many lines change → docs.
  - Formatting only: whitespace, line breaks or import order with identical logic → style.
  - Moved or renamed files with little or no content change → refactor (or chore for non-source files).
  - Changes limited to tests, CI files, build files or dependency manifests → test, ci, build or deps.
- To tell documentation from code, check whether any executable line changed. If only docstrings and comments changed, the type is docs, never feat or fix.
- For mixed changes, choose the type of the most significant change for users or behavior, and describe the other groups in the body bullets.
- Do not describe a new feature when the diff only modifies existing code, and do not describe a fix when the diff only adds new code.
- Set the scope to the module, package or directory shared by the staged files. If the files span unrelated areas, use the nearest common package or omit the scope.
- Write one body bullet per distinct change, naming real functions, classes, files or columns from the diff. Use "Add" for new code, "Update" or "Fix" for modified code, and "Remove" for deleted code.
- Treat a removed or renamed public function, a changed public signature or a changed return value as a breaking change: use "!" and the BREAKING CHANGE footer.
- Do not state intent, causes or impact that cannot be confirmed from the diff.

🎯 Be specific, concise, and always write in English. Use model's analytical capabilities to understand the code changes and generate contextually appropriate commit messages.
