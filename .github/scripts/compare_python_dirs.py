import filecmp
import sys
from pathlib import Path


def collect_py_files(directory):
    """Collect all .py files in a directory recursively, returning relative paths."""
    directory = Path(directory).resolve()
    return {f.relative_to(directory): f for f in directory.rglob("*.py") if f.is_file()}


def compare_directories(dir1, dir2):
    files1 = collect_py_files(dir1)
    files2 = collect_py_files(dir2)

    all_keys = set(files1.keys()).union(files2.keys())
    all_match = True
    out = []

    for rel_path in sorted(all_keys):
        f1 = files1.get(rel_path)
        f2 = files2.get(rel_path)

        if f1 is None:
            out.append(f"  Only in {dir2}: {rel_path}")
            all_match = False
        elif f2 is None:
            out.append(f"  Only in {dir1}: {rel_path}")
            all_match = False
        elif not filecmp.cmp(f1, f2, shallow=False):
            out.append(f"  Files differ: {rel_path}")
            all_match = False

    if out:
        print("\n".join(sorted(out)))

    return all_match


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_dirs.py <dir1> <dir2>")
        sys.exit(2)

    dir1, dir2 = sys.argv[1], sys.argv[2]

    print(f"Comparing Python files between {dir1} and {dir2}")

    if compare_directories(dir1, dir2):
        print("All .py files match.")
        sys.exit(0)
    else:
        print("Some .py files differ.")
        sys.exit(1)
