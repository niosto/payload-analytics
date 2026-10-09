#!/bin/bash

COLOR_BLUE=$(tput setaf 4)
COLOR_DEFAULT=$(tput sgr0)

git config --global --add safe.directory /app
git config --global core.autocrlf false
git config --global core.eol lf
