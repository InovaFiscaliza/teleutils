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

🎯 Be specific, concise, and always write in English. Use model's analytical capabilities to understand the code changes and generate contextually appropriate commit messages."