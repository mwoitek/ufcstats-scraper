#!/bin/bash
# Script that scrapes fighter details for a range of letters

if [[ -z "$VIRTUAL_ENV" ]]; then
  echo 'Activate virtual environment first!'
  exit 1
fi

curr_dir="$(realpath "$0" | xargs -I{} dirname {})"
py_script="$(realpath "${curr_dir}/../src/fighter_details.py")"

if [[ ! -f "$py_script" ]]; then
  echo "Could not find Python script ${py_script}!"
  exit 1
fi

letters="$1"
delay="$2"

delay_arg=''
[[ -n "$delay" ]] && delay_arg=" --delay ${delay}"

if [[ -n "$letters" ]]; then
  for ((i = 0; i < ${#letters}; i++)); do
    letter="${letters:$i:1}"
    echo -e "python ${py_script} ${letter}${delay_arg}\n"
    # shellcheck disable=SC2086
    python "$py_script" "$letter"$delay_arg
    ((i < ${#letters} - 1)) && echo
  done
else
  for letter in {a..z}; do
    echo -e "python ${py_script} ${letter}${delay_arg}\n"
    # shellcheck disable=SC2086
    python "$py_script" "$letter"$delay_arg
    [[ "$letter" != 'z' ]] && echo
  done
fi
