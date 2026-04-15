#!/bin/bash

git diff --name-only --cached | xargs -I {} echo -i "{}" | xargs code2prompt -c .
