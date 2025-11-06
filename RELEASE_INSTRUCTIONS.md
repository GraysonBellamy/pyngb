# Release Instructions for v0.1.1

## ✅ Completed Steps

1. ✅ Version bumped to 0.1.1 in `pyproject.toml`
2. ✅ CHANGELOG.md created with detailed release notes
3. ✅ All changes committed with descriptive message
4. ✅ Git tag `v0.1.1` created
5. ✅ Package built successfully:
   - `dist/pyngb-0.1.1-py3-none-any.whl` (69K)
   - `dist/pyngb-0.1.1.tar.gz` (1.8M)
6. ✅ Package validated with twine (PASSED)

## 🚀 Next Steps (Manual)

### 1. Push to GitHub

```bash
# Push the commit
git push origin main

# Push the tag
git push origin v0.1.1
```

### 2. Create GitHub Release

Go to https://github.com/GraysonBellamy/pyngb/releases/new

**Release details:**
- **Tag:** `v0.1.1`
- **Title:** `Release v0.1.1: Migrate to pathlib.Path`
- **Description:** Use the content from `RELEASE_NOTES_v0.1.1.md`

Or use GitHub CLI:

```bash
gh release create v0.1.1 \
  --title "Release v0.1.1: Migrate to pathlib.Path" \
  --notes-file RELEASE_NOTES_v0.1.1.md \
  dist/pyngb-0.1.1-py3-none-any.whl \
  dist/pyngb-0.1.1.tar.gz
```

### 3. Publish to PyPI

#### Test PyPI (optional, recommended first):

```bash
uv tool run twine upload --repository testpypi dist/pyngb-0.1.1*
```

Then verify:
```bash
pip install --index-url https://test.pypi.org/simple/ pyngb==0.1.1
```

#### Production PyPI:

```bash
uv tool run twine upload dist/pyngb-0.1.1*
```

You'll be prompted for your PyPI credentials, or you can use a token:
- Username: `__token__`
- Password: Your PyPI API token

#### Using API Token (Recommended):

Set up your `~/.pypirc`:
```ini
[pypi]
username = __token__
password = pypi-YOUR-API-TOKEN-HERE
```

Then upload:
```bash
uv tool run twine upload dist/pyngb-0.1.1*
```

### 4. Verify Release

After publishing to PyPI:

```bash
# Install from PyPI
pip install --upgrade pyngb

# Verify version
python -c "import pyngb; print(pyngb.__version__)"
# Should output: 0.1.1

# Test basic functionality
python -c "from pathlib import Path; from pyngb import read_ngb; print('Import successful!')"
```

## 📋 Release Checklist

- [ ] Push commit to GitHub (`git push origin main`)
- [ ] Push tag to GitHub (`git push origin v0.1.1`)
- [ ] Create GitHub release with release notes
- [ ] Upload to Test PyPI (optional)
- [ ] Verify Test PyPI installation (optional)
- [ ] Upload to Production PyPI
- [ ] Verify PyPI installation
- [ ] Update any documentation that references version numbers
- [ ] Announce release (if applicable)

## 🔍 Pre-Release Verification

All checks have passed:
- ✅ All 384 tests passing
- ✅ Package builds successfully
- ✅ Twine validation passed
- ✅ No linting errors
- ✅ Version numbers consistent across files
- ✅ CHANGELOG.md updated
- ✅ Release notes prepared

## 📝 Release Summary

**Version:** 0.1.1
**Release Date:** 2025-01-06
**Release Type:** Minor (backwards compatible enhancement)
**Key Changes:** Pathlib migration, bug fixes, test improvements
**Tests:** 384 passing, 0 failing
**Backwards Compatible:** Yes ✅

## 🛠️ Troubleshooting

### If twine upload fails with authentication error:

1. Generate a PyPI API token at https://pypi.org/manage/account/token/
2. Use `__token__` as username and your token as password
3. Or configure `~/.pypirc` as shown above

### If git push fails:

Check your SSH keys:
```bash
ssh -T git@github.com
```

### If package import fails after installation:

```bash
pip install --upgrade --force-reinstall pyngb==0.1.1
```

## 📞 Support

For issues with the release process, contact the maintainer or open an issue on GitHub.
