#!/usr/bin/env python3
import copy
import shutil
import subprocess
from datetime import datetime
from os import mkdir, system

CMAKE_PATH = "~/intelliJ/clion-2022.2.1/bin/cmake/linux/bin/cmake"
HOST = "cascade-01"
TPCC = True

FEATURES = {
    # "head-early-abort-create": ["true", "false"],
    "head-early-abort-create": ["false"],
    "inner": ["basic", "padded", "explicit_length", "ascii", "art"],
    # "inner": ["basic"],
    "leaf": ["basic", "hash", "adapt"],
    # "leaf" : ["hash","basic"],
    # "hash-leaf-simd": ["32", "64"],
    "hash-leaf-simd": ["32"],
    # "strip-prefix": ["true", "false"],
    "strip-prefix": ["false", "true"],
    # "hash": ["crc32","wyhash", "fx"],
    "hash": ["crc32"],
    "descend-adapt-inner": ["none", "1000", "100", "10"],
    "branch-cache": ["false", "true"],
    "dynamic-prefix": ["false", "true"],
    "hash-variant": ["head", "alloc"],
    "leave-adapt-range": ["3", "7", "15", "31"],

    "basic-use-hint": ["false", "true", "naive"],
    "basic-prefix": ["false", "true"],
    "basic-heads": ["false", "true"],
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


def build_all(cfg_set, tpcc=TPCC):
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
        if tpcc:
            assert system(f'cd tpcc; make') == 0
            shutil.copyfile("tpcc/tpcc.elf", f'{build_dir}/btree-{revision}-{counter}')
        else:
            assert system(f'cargo rustc --bin btree --release -- -C target-cpu=cascadelake') == 0
            shutil.copyfile("target/release/btree", f'{build_dir}/btree-{revision}-{counter}')
    return build_dir


def upload(dir):
    shutil.copyfile("run_all_cp_target.sh", f'{dir}/run_all.sh')
    system(f"ssh {HOST} rm -r cp-target")
    assert system(f'rsync -r -E -e ssh {dir}/ {HOST}:cp-target/ ') == 0


def print_uploaded():
    print(f"ssh -f {HOST} 'nohup bash cp-target/run_all.sh'")


features = default_features()
cases = [copy.deepcopy(features)]


def set_feature(k, v):
    assert k in features
    assert v in FEATURES[k]
    features[k] = v
    cases.append(copy.deepcopy(features))


set_feature('basic-prefix', 'true')
set_feature('basic-heads', 'true')
set_feature('basic-use-hint', 'true')
# set_feature('dynamic-prefix', 'true')
# features['dynamic-prefix'] = "false"
set_feature("leaf", "hash")
# set_feature("strip-prefix", "true")
# features["strip-prefix"] = "false"
# set_feature("branch-cache", 'true')
# features["branch-cache"] = "false"
cases = []
set_feature('inner', "explicit_length")
# for inner in ["padded", "explicit_length", "ascii", "art"]:
#    set_feature('inner', inner)
set_feature("leaf", "adapt")
# for adapt in ["1000", "100", "10"]:
#    set_feature("descend-adapt-inner", adapt)

assert len(cases) == 2

dir = build_all(cases)
upload(dir)
print_uploaded()
# configure(default_features())
