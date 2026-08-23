"""Dependency-free runner: executes every test_* callable in tests/test_*.py.
Works without pytest; pytest can also collect these files directly.

Usage:  python -m tests.run_all
Exit code 0 = all green, 1 = failures."""
import asyncio
import importlib
import sys
import traceback
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))


def discover():
    modules = []
    for f in sorted(HERE.glob('test_*.py')):
        modules.append(importlib.import_module(f'tests.{f.stem}'))
    return modules


def run():
    passed, failed = 0, []
    for mod in discover():
        for name in sorted(dir(mod)):
            if not name.startswith('test_'):
                continue
            fn = getattr(mod, name)
            if not callable(fn):
                continue
            try:
                result = fn()
                if asyncio.iscoroutine(result):
                    asyncio.run(result)
                passed += 1
                print(f'  PASS {mod.__name__}.{name}')
            except Exception:
                failed.append((f'{mod.__name__}.{name}', traceback.format_exc()))
                print(f'  FAIL {mod.__name__}.{name}')

    print(f'\n{passed} passed, {len(failed)} failed')
    if failed:
        for name, tb in failed:
            print(f'\n===== {name} =====\n{tb}')
        sys.exit(1)


if __name__ == '__main__':
    run()
