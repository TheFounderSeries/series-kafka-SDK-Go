# GitHub Setup & Installation Guide

## Step 1: Create GitHub Repository

1. **Go to GitHub**: https://github.com/TheFounderSeries
2. **Click "New Repository"**
3. **Repository Settings**:
   - Repository name: `series-kafka-SDK-Go`
   - Description: `Production-ready Kafka SDK for Go with DLQ, schema validation, and OpenTelemetry support`
   - Visibility: `Public` (so others can `go get` it)
   - **DO NOT** initialize with README, .gitignore, or license (we already have these)
4. **Click "Create repository"**

## Step 2: Push Code to GitHub

After creating the repository, run these commands:

```bash
cd /Users/sethvin-nanayakkara/Series/kafka-SDKs/series-kafka-SDK-Go

# Add the remote repository
git remote add origin git@github.com:TheFounderSeries/series-kafka-SDK-Go.git

# Or if you prefer HTTPS:
# git remote add origin https://github.com/TheFounderSeries/series-kafka-SDK-Go.git

# Push to GitHub
git branch -M main
git push -u origin main
```

## Step 3: How Others Install Your SDK (Go's "pip install" equivalent)

### For Go, installation is SUPER simple!

Unlike Python's `pip install`, Go uses `go get` which downloads directly from GitHub:

```bash
# Users install your SDK with ONE command:
go get github.com/TheFounderSeries/series-kafka-SDK-Go
```

That's it! No package registry needed (like PyPI for Python).

### How It Works

1. **Your `go.mod` file** already has the correct module path:
   ```go
   module github.com/TheFounderSeries/series-kafka-SDK-Go
   ```

2. **When someone runs** `go get github.com/TheFounderSeries/series-kafka-SDK-Go`:
   - Go fetches the code directly from your GitHub repo
   - Adds it to their `go.mod` file
   - Downloads all dependencies automatically

3. **They can then use it**:
   ```go
   import "github.com/TheFounderSeries/series-kafka-SDK-Go/pkg/core"
   import "github.com/TheFounderSeries/series-kafka-SDK-Go/topics/messages"
   ```

## Step 4: Create a Release (Optional but Recommended)

### Why Create Releases?

Go supports **semantic versioning** via Git tags. Users can install specific versions:

```bash
# Install specific version
go get github.com/TheFounderSeries/series-kafka-SDK-Go@v1.0.0

# Install latest
go get github.com/TheFounderSeries/series-kafka-SDK-Go@latest
```

### Creating Your First Release

#### Option A: Via GitHub UI (Easiest)

1. Go to your repo: https://github.com/TheFounderSeries/series-kafka-SDK-Go
2. Click "Releases" → "Create a new release"
3. Click "Choose a tag" → Type `v1.0.0` → "Create new tag"
4. Title: `v1.0.0 - Initial Release`
5. Description:
   ```markdown
   ## Features
   - ✅ Producer with idempotent delivery
   - ✅ Consumer with worker pools and DLQ
   - ✅ Subset field extraction for performance
   - ✅ OpenTelemetry tracing & metrics
   - ✅ Schema validation and contracts
   - ✅ Comprehensive tests
   ```
6. Click "Publish release"

#### Option B: Via Command Line

```bash
cd /Users/sethvin-nanayakkara/Series/kafka-SDKs/series-kafka-SDK-Go

# Create and push tag
git tag -a v1.0.0 -m "Initial release: Series Kafka Go SDK v1.0.0"
git push origin v1.0.0
```

Then create the release on GitHub UI as described above.

## Step 5: Update Your README (Already Done!)

Your README.md already has installation instructions:

```markdown
## Installation

\`\`\`bash
go get github.com/TheFounderSeries/series-kafka-SDK-Go
\`\`\`
```

## Key Differences: Go vs Python Package Distribution

| Aspect | Python (pip) | Go (go get) |
|--------|-------------|-------------|
| **Registry** | Requires PyPI account | Uses GitHub directly |
| **Installation** | `pip install package-name` | `go get github.com/user/repo` |
| **Versioning** | setup.py / pyproject.toml | Git tags (v1.0.0) |
| **Publishing** | `twine upload` | Just push to GitHub |
| **Dependencies** | Listed in requirements.txt | Managed in go.mod |

## Go Module Best Practices

### Versioning Strategy

- **v1.x.x**: Stable API, backward compatible changes only
- **v2.x.x**: Breaking changes → Create `/v2` subdirectory or new repo

### When to Increment Versions

- **v1.0.0 → v1.0.1**: Bug fixes (PATCH)
- **v1.0.0 → v1.1.0**: New features, backward compatible (MINOR)
- **v1.0.0 → v2.0.0**: Breaking API changes (MAJOR)

### Creating New Versions

```bash
# After making changes and committing:
git tag -a v1.1.0 -m "Add new feature X"
git push origin v1.1.0
```

Users can then:
```bash
go get github.com/TheFounderSeries/series-kafka-SDK-Go@v1.1.0
```

## Verification After Publishing

Once pushed to GitHub, verify your module is accessible:

```bash
# Check if Go can find your module
go list -m github.com/TheFounderSeries/series-kafka-SDK-Go@latest

# See available versions
go list -m -versions github.com/TheFounderSeries/series-kafka-SDK-Go
```

## Documentation

### Go.dev Documentation (Automatic!)

Your package will automatically appear on **pkg.go.dev**:
- URL: https://pkg.go.dev/github.com/TheFounderSeries/series-kafka-SDK-Go
- Updates automatically when you push to GitHub
- Extracts documentation from your code comments

Make sure all exported functions have doc comments:
```go
// NewProducer creates a new Kafka producer with the given configuration.
// It returns an error if the configuration is invalid or if the producer
// cannot be initialized.
func NewProducer(config *ProducerConfig) (*Producer, error) {
    // ...
}
```

## Next Steps

1. ✅ Create GitHub repo
2. ✅ Push code
3. ✅ Create v1.0.0 release
4. ✅ Test installation: `go get github.com/TheFounderSeries/series-kafka-SDK-Go`
5. ✅ Check pkg.go.dev documentation (appears automatically)
6. ✅ Share with your team!

## Support

For issues or questions:
- GitHub Issues: https://github.com/TheFounderSeries/series-kafka-SDK-Go/issues
- Discussions: https://github.com/TheFounderSeries/series-kafka-SDK-Go/discussions

