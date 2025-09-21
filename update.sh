#!/bin/bash

# Prompt nhập tên
read -p "Type your name: " NAME

# Lấy timestamp hiện tại
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Commit message
MESSAGE="$TIMESTAMP Update pipeline from $NAME"

# Thực hiện git
git add .
git commit -m "$MESSAGE"
git push origin main
