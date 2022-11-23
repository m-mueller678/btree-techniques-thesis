import shutil
import subprocess
from datetime import datetime
from os import mkdir, system

CMAKE_PATH = "~/intelliJ/clion-2022.2.1/bin/cmake/linux/bin/cmake"
HOST = "cascade-01"

FEATURES = {
    # "head-early-abort-create": ["true", "false"],
    "head-early-abort-create": ["false"],
    # "inner": ["padded", "basic", "explicit_length", "ascii"],
    "inner": ["basic"],
    "leaf": ["hash"],
    # "leaf" : ["hash","basic"],
    # "hash-leaf-simd": ["32", "64"],
    "hash-leaf-simd": ["32"],
    # "strip-prefix": ["true", "false"],
    "strip-prefix": ["false"],
    "hash": ["wyhash", "fx", "crc32"],
}

def configure(chosen_features, revision=None):
    shutil.copyfile("Cargo.toml", "Cargo.toml.old")
    with open("Cargo.toml.old") as src:
        with open("Cargo.toml", "w") as dst:
            for l in src.readlines():
                dst.write(l)
                if '[features]' in l:
                    break
            default_features = ','.join([f'"{k}_{chosen_features[k]}"' for k in chosen_features.keys()])
            dst.write(f'default = [{default_features}]\n')
            for feature in FEATURES.keys():
                for option in FEATURES[feature]:
                    dst.write(f"{feature}_{option} = []\n")
    if revision is not None:
        chosen_features = chosen_features.copy()
        chosen_features['revision'] = revision
    keys = list(chosen_features.keys())
    header = ','.join(keys)
    print(chosen_features)
    values = ','.join(chosen_features[k] for k in keys)
    with open('build-info.h', 'w') as dst:
        dst.write(f'auto BUILD_CSV_HEADER = ",{header}";\n')
        dst.write(f'auto BUILD_CSV_VALUES = ",{values}";\n')


def default_features():
    out = {}
    for k in FEATURES.keys():
        out[k] = FEATURES[k][0]
    return out


def all_feature_combinations():
    out = [{}]
    for k in FEATURES.keys():
        out2 = []
        for cfg in out:
            for v in FEATURES[k]:
                cfg2 = cfg.copy()
                cfg2[k] = v
                out2.append(cfg2)
        out = out2
    return out


def build_all(cfg_set):
    system("git status")
    input()
    time = str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    build_dir = f'build-{time}'
    mkdir(build_dir)
    counter = 0
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip()
    for cfg in cfg_set:
        counter = counter + 1
        configure(cfg, revision)
        assert system(f'{CMAKE_PATH} --build cmake-build-release --target btree -j 3') == 0
        shutil.copyfile("cmake-build-release/btree", f'{build_dir}/btree-{revision}-{counter}')
    return build_dir


def upload(dir):
    shutil.copyfile("run_all_cp_target.sh", f'{dir}/run_all.sh')
    system(f"ssh {HOST} rm -r cp-target")
    assert system(f'rsync -r -E -e ssh {dir}/ {HOST}:cp-target/ ') == 0


def print_uploaded():
    print(f"ssh -f {HOST} 'nohup bash cp-target/run_all.sh'")


dir = build_all(all_feature_combinations())
upload(dir)
print_uploaded()
