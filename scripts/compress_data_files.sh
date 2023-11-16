#!/bin/bash
# Script for compressing scraped data

curr_dir="$(realpath "$0" | xargs -I{} dirname {})"
data_dir="$(realpath "${curr_dir}/../data")"

time_ns="$(python -c 'import time; print(time.time_ns())')"

if command -v trash-put &>/dev/null; then
  rm_cmd='trash-put'
else
  rm_cmd='rm'
fi

compress_dir() {
  d="$1"
  e="$2"

  if [[ -z "$d" ]]; then
    echo 'Directory not specified!' >&2
    return 1
  fi

  if [[ ! -d "$d" ]]; then
    echo "Directory ${d} not found!" >&2
    return 1
  fi

  if [[ -z "$e" ]]; then
    echo 'File extension not specified!' >&2
    return 1
  fi

  echo -e "Directory: ${d}\n"

  has_files="$(find "$d" -type f -name "*.${e}")"
  if [[ -z "$has_files" ]]; then
    echo 'There is no file to compress!'
    return 0
  fi

  echo -e "Compressing ${e^^} files...\n"
  out_file="${d}/$(basename "$d")_${time_ns}.tar.gz"
  tar -czvf "$out_file" "$d"/*."$e" 2>/dev/null

  # shellcheck disable=SC2181
  if [[ $? -ne 0 ]]; then
    echo 'Failed to compress files!' >&2
    return 1
  fi
  echo -e "\nCreated file ${out_file}!\n"

  echo -e 'Removing original files...\n'
  "$rm_cmd" -v "$d"/*."$e"
}

compress_dir "${data_dir}/fighter_details" 'json'
echo
compress_dir "${data_dir}/fighters_list" 'json'
echo
compress_dir "${data_dir}/links/fighters" 'txt'
