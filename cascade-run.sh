#!/bin/bash

ssh cascade-01 rm -rf cp-target
rsync -e ssh . cascade-01:~/cp-target -r -E --exclude .git
ssh -t cascade-01 'cd cp-target; bash -l'