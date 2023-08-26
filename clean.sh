#!/usr/bin/env bash

rm -rf .git

git init
git add .
git commit -m "Initial commit"

git remote add origin https://github.com/tobias-gaenzler/kafka-streams-dead-letter-publishing
git config user.name "Tobias"
git config user.email "1056318+tobias-gaenzler@users.noreply.github.com"
git push -u --force origin main
