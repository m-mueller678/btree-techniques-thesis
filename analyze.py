import sys

import pandas as pd

KEY_TYPES = {
    'basic-heads': 'build', 'basic-prefix': 'build', 'basic-use-hint': 'build', 'branch-cache': 'build', 'data': 'run',
    'descend-adapt-inner': 'build', 'dynamic-prefix': 'build', 'hash': 'build', 'hash-leaf-simd': 'build',
    'head-early-abort-create': 'build', 'host': 'run', 'inner': 'build', 'leaf': 'build', 'op': 'run',
    'op_count': 'val',
    'op_rates': 'run', 'range_len': 'run', 'revision': 'build', 'run_start': 'aux', 'strip-prefix': 'build',
    'time': 'val', 'total_count': 'run', 'value_len': 'run', 'zipf_exponent': 'run', 'branch_misses': 'val',
    'cycles': 'val', 'instructions': 'val', 'l1d_misses': 'val', 'l1i_misses': 'val', 'll_misses': 'val',
    'task_clock': 'val'
}


def default_pivot(dt, aggregate=[]):
    for k in aggregate:
        assert KEY_TYPES[k] in ['build', 'run']
    for k in dt.columns:
        assert KEY_TYPES[k] in ['build', 'run', 'val', 'aux']
    build = sorted([k for k in KEY_TYPES if KEY_TYPES[k] == 'build' and k not in aggregate])
    run = sorted([k for k in KEY_TYPES if KEY_TYPES[k] == 'run' and k not in aggregate])
    index = build + run
    index = [k for k in index if len(pd.unique(dt[k])) != 1]
    values = sorted([k for k in KEY_TYPES if KEY_TYPES[k] == 'val'])
    return dt.pivot_table(values=values, index=index)


def load(f):
    dt = pd.read_json(f, lines=True)
    dt['op_rates'] = dt['op_rates'].map(lambda x: ':'.join(str(r) for r in x))
    dt['host'] = dt['host'].map(lambda x: x.strip())
    return dt


path = sys.argv[1] if len(sys.argv) > 1 else 'out.out'
print(
    default_pivot(load(path), ['op'])
)
