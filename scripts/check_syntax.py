#!/usr/bin/env python3
import py_compile
import glob
import sys
import traceback
import os


def main():
    errs = False
    files = glob.glob('**/*.py', recursive=True)
    print(f'Checking {len(files)} python files...')
    for f in files:
        try:
            py_compile.compile(f, doraise=True)
        except Exception:
            print('ERROR in', f)
            traceback.print_exc()
            errs = True
    if not errs:
        print('Python compile: OK')

    try:
        import jinja2
        env = jinja2.Environment()
        templates = []
        for root, _, fs in os.walk('app'):
            for fn in fs:
                if fn.endswith('.html') or fn.endswith('.jinja2'):
                    templates.append(os.path.join(root, fn))
        print(f'Checking {len(templates)} templates...')
        for t in templates:
            try:
                src = open(t, 'r', encoding='utf-8').read()
                env.parse(src)
            except Exception:
                print('TEMPLATE ERROR', t)
                traceback.print_exc()
                errs = True
        if not errs:
            print('Templates: OK')
    except ImportError:
        print('jinja2 not installed; skipping template parse. To check templates, run: pip install jinja2')

    if errs:
        sys.exit(2)


if __name__ == '__main__':
    main()
