#!/bin/bash

#
# 操作 rclone 进行文件备份，将生产环境中的文件备份到 S3 等服务器中。
# 

ID_ALL="all"
DIR_NOT_FOUND="directory not found"
INCREMENTAL="incremental"
CMD_LIST=("backup" "delete" "forget" "help" "info" "init" "list" "remove" "restore" "rm_before" "rmlock" "size" "snapshot" "snapshots")
STORE_SNAPSHOTS_FILE=
TEMP_SNAPSHOTS_FILE=
TEMP_SNAPSHOTS_FILE_NEW=

show_usage(){
  echo "Usage:
  $0 <store_path> <command> [options...]

Available Commands:
  backup       Backup the source path to the backup strore path.
  delete       Delete the backup strore path.
  forget       Remove snapshots according to a policy.
  help         Help about any command.
  info         Show the information of the backup strore path.
  init         Initialize the backup strore path.
  list         List content in a snapshot.
  remove       Remove a snapshot.
  restore      Restore a snapshot.
  rm_before    Remove backup content before a datetime.
  rmlock       Remove the lock file manually.
  size         Show the size of the backup strore path.
  snapshot     Show a snapshot info.
  snapshots    List snapshots.
"
}

show_usage_backup(){
  echo "Usage:
  $0 <store_path> backup <source_path> [options...]
"
}

show_usage_delete(){
  echo "Usage:
  $0 <store_path> delete [-f]

Available Options:
  -f: force delete, not ask for confirmation
"
}

show_usage_forget(){
  echo "
The "forget" command removes snapshots according to a policy.

Usage:
  $0 <store_path> forget [options...]

Available Options:
  -l, --keep-last n                    keep the last n snapshots
  -H, --keep-hourly n                  keep the last n hourly snapshots
  -d, --keep-daily n                   keep the last n daily snapshots
  -w, --keep-weekly n                  keep the last n weekly snapshots
  -m, --keep-monthly n                 keep the last n monthly snapshots
  -y, --keep-yearly n                  keep the last n yearly snapshots
      --keep-within duration           keep snapshots that are newer than duration (eg. 1y5m7d2h) relative to the latest snapshot
      --keep-within-hourly duration    keep hourly snapshots that are newer than duration (eg. 1y5m7d2h) relative to the latest snapshot
      --keep-within-daily duration     keep daily snapshots that are newer than duration (eg. 1y5m7d2h) relative to the latest snapshot
      --keep-within-weekly duration    keep weekly snapshots that are newer than duration (eg. 1y5m7d2h) relative to the latest snapshot
      --keep-within-monthly duration   keep monthly snapshots that are newer than duration (eg. 1y5m7d2h) relative to the latest snapshot
      --keep-within-yearly duration    keep yearly snapshots that are newer than duration (eg. 1y5m7d2h) relative to the latest snapshot
"
}

show_usage_help(){
  echo "Usage:
  $0 help [command]
"
}

show_usage_info(){
  echo "Usage:
  $0 <store_path> info
"
}

show_usage_init(){
  echo "Usage:
  $0 <store_path> init [-i] [options...]

Available Options:
  -i: incremental backup (default: full backup)
  other options suported by rclone copy/sync
"
}

show_usage_list(){
  echo "Usage:
  $0 <store_path> list <snapshot_id> [path]
"
}

show_usage_remove(){
  echo "Usage:
  $0 <store_path> remove [-f] <snapshot_id>

Available Options:
  -f: force remove snapshot, not ask for confirmation
"
}

show_usage_restore(){
  echo "Usage:
  $0 <store_path> restore <snapshot_id> <backup_path> <restore_path>
"
}

show_usage_rm_before(){
  echo "Usage:
  $0 <store_path> rm_before [-f] <duration | datetime>

Available Options:
  -f: force remove content, not ask for confirmation
"
}

show_usage_rmlock(){
  echo "Usage:
  $0 <store_path> rmlock [-f]

Available Options:
  -f: force remove lock, not ask for confirmation
"
}

show_usage_size(){
  echo "Usage:
  $0 <store_path> size
"
}

show_usage_snapshot(){
  echo "Usage:
  $0 <store_path> snapshot <snapshot_id>
"
}

show_usage_snapshots(){
  echo "Usage:
  $0 <store_path> snapshots
"
}

rclone_show_cmd(){
  echo "rclone $@"
  rclone "$@" 
}

# 执行rclone命令，如果失败则输出错误信息并返回错误码
# 在需要返回值的方法中使用，以避免影响返回值
rclone_silent(){
  local out
  local status

  out=$(rclone "$@" 2>&1)
  status=$?

  [ $status -ne 0 ] && { echo "$out"; return $status; }
  return 0
}

# 对目标库加锁，防止其他进程操作
lock(){
  local out
  
  echo "lock" > ${temp_dir}/caman.lock
  out=$(rclone copyto ${temp_dir}/caman.lock ${store_path}/caman-meta/caman.lock --suffix=.bak -v --ignore-size --ignore-times --ignore-checksum 2>&1)

  if [ $? -eq 0 ]; then
    # 没有生成 .bak 文件，说明目标库中原来没有 caman.lock 文件
    if ! echo "$out" | grep -q "to: caman.lock.bak"; then
      return 0
    fi
  else
    echo "$out"
  fi

  echo "lock $store_path failed"
  return 1
}

# 对目标库解锁，允许其他进程操作
unlock(){
  rclone delete "${store_path}/caman-meta/caman.lock" && return 0
  echo "unlock $store_path failed"
  return 1
}

# 获取快照信息文件，取下的文件放在 TEMP_SNAPSHOTS_FILE
get_snapshots_file(){
  rclone copyto "$STORE_SNAPSHOTS_FILE" "$TEMP_SNAPSHOTS_FILE"
}

# 将快照信息文件上传到目标库，新信息须放在 STORE_SNAPSHOTS_FILE_NEW
put_snapshots_file(){
  rclone copyto "$TEMP_SNAPSHOTS_FILE_NEW" "$STORE_SNAPSHOTS_FILE"
}

get_log_file_path(){
  for ((i = 1; i <= $#; i++)); do
    if [ "${!i}" == "--log-file" ]; then
      next_index=$((i + 1))
      echo "${!next_index}"
      break
    elif [[ "${!i}" == --log-file=* ]]; then
      echo "${!i#*=}"
      break
    fi
  done
}

# 返回两个大小：本次备份比上次备份增加的大小，本次备份上传的大小
get_added_size(){
  local log_file="$1"
  local size_num_tr=0
  local size_unit_tr=""
  local size_num_cp=0
  local size_unit_cp=""
  local size_num_mv=0
  local size_unit_mv=""
  read size_num_tr size_unit_tr <<< $(tail -n 100 "$log_file" | grep "Transferred:.*ETA" | tail -n 1 | awk -F'Transferred:' '{print $2}' | awk '{print $1, $2}')
  read size_num_cp size_unit_cp <<< $(tail -n 100 "$log_file" | grep "Server Side Copies:" | tail -n 1 | awk '{print $6, $7}')
  read size_num_mv size_unit_mv <<< $(tail -n 100 "$log_file" | grep "Server Side Moves:" | tail -n 1 | awk '{print $6, $7}')

  local size_tr_bytes=$(get_size_bytes "$size_num_tr" "$size_unit_tr")
  local size_cp_bytes=$(get_size_bytes "$size_num_cp" "$size_unit_cp")
  local size_mv_bytes=$(get_size_bytes "$size_num_mv" "$size_unit_mv")
  echo "$((size_tr_bytes - size_cp_bytes - size_cp_bytes - size_mv_bytes))" "$((size_tr_bytes - size_cp_bytes))"
}

get_size_bytes(){
  local size_num="$1"
  local size_unit="$2"
  case "$size_unit" in
    "B")
      echo "$size_num"
      ;;
    "KiB")
      awk "BEGIN {printf \"%.0f\", $size_num * 1024}"
      ;;
    "MiB")
      awk "BEGIN {printf \"%.0f\", $size_num * 1024 * 1024}"
      ;;
    "GiB")
      awk "BEGIN {printf \"%.0f\", $size_num * 1024 * 1024 * 1024}"
      ;;
    "TiB")
      awk "BEGIN {printf \"%.0f\", $size_num * 1024 * 1024 * 1024 * 1024}"
      ;;
    *)
      echo "0"
      ;;
  esac
}

# Filter out some argument and its value
filter_option() {
  local option="$1"
  shift

  local args=("$@")
  local filtered_args=()
  for arg in "${args[@]}"; do
    if [[ "$arg" == "$option" ]]; then
      # Skip the next argument (the path)
      skip_next=1
    elif [[ "$arg" == ${option}=* ]]; then
      # Skip this argument as it includes the path
      continue
    elif [[ -n "$skip_next" ]]; then
      # Clear the skip_next flag and skip this argument
      unset skip_next
    else
      # Otherwise, add to the filtered arguments
      filtered_args+=("$arg")
    fi
  done

  # Return the filtered arguments as a string
  echo "${filtered_args[@]}"
}

# Check if the --log-file argument is present in the arguments
has_log_file() {
  local args=("$@")
  for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[i]}" == --log-file=* ]]; then
      return 0
    elif [[ "${args[i]}" == "--log-file" && $((i + 1)) -lt ${#args[@]} ]]; then
      # 检查下一个参数是否存在且不是一个选项
      local next_arg="${args[i + 1]}"
      if [[ -n "$next_arg" && "$next_arg" != --* ]]; then
        return 0
      fi
    fi
  done
  return 1  # 未找到匹配项
}

# 判断目标路径是不是一个备份库，返回 0 表示是，1 表示否
is_backup(){
  local name
  if ! name=$(rclone lsf "$store_path/caman-meta/snapshots" 2>/dev/null); then
    return 1  # 不是备份库
  fi
  [ -n "$name" ]
}

# 判断目录是否为空，不存在也视为空
is_empty_dir(){
  local dir="$1"
  g_out=$(rclone lsjson "$dir" 2>&1)
  local status=$?

  # 目录不存在，视为空目录
  if echo "$g_out" | grep -q "$DIR_NOT_FOUND"; then
    return 0
  fi

  # 如果执行 rclone 有问题
  if [ $status -ne 0 ]; then
    echo $g_out
    return 1
  fi

  if [ "$(echo "$g_out" | jq -r '. | length')" -ne 0 ]; then
    echo "$dir is not empty"
    return 1
  fi
}

# 判断快照 ID 是否为空或无效
is_snapshot_empty(){
  local snapshot_id="$1"
  local show_usage_fun="$2"
  if [ ${#snapshot_id} -lt 8 ]; then
    echo "Snapshot id must be at least 8 characters"
    echo
    $show_usage_fun
    return 1
  fi
}

# 执行一个 rclone 命令，忽略目录不存在的错误
rclone_ignore_not_found(){
  g_out=$(rclone "$@" 2>&1)
  local status=$?

  if [ $status -ne 0 ] && echo "$g_out" | grep -q "$DIR_NOT_FOUND"; then
    return 0
  fi

  [ -z "$g_out" ] || echo "$g_out"
  return $status
}

# 判断一个 snapshot 是否存在
# 调用本方法之前，先下载 snapshot 列表到 $TEMP_SNAPSHOTS_FILE
is_snapshot_exist(){
  local snapshot_id="$1"

  g_out=$(jq -s '[.[] | select(.id | startswith("'$snapshot_id'"))]' "$TEMP_SNAPSHOTS_FILE")
  if [ $? -ne 0 ]; then
    echo $g_out
    return 1
  else
    local num=$(echo "$g_out" | jq -r '. | length')
    if [ $num -ne 1 ]; then
      echo "Snapshot $snapshot_id not found"
      return 1
    fi
  fi
}

# 取得完整的 snapshot id
get_whole_snapshot_id(){
  local snapshot_id="$1"
  jq -r -s '.[] | select(.id | startswith("'$snapshot_id'")) | .id' "$TEMP_SNAPSHOTS_FILE"
}

# 取得匹配目录的 filter 选项
# 目录是 yyyymmddhhmmss 格式，要取得时间比目录晚的那些目录
# 传进来的 path 是相对路径，需要前后都不带斜杠 /
gen_filter_file(){
  local dir="$1"
  local file="$2"
  local path="$3"

  if [ -z $path ]; then
    path="**"
  else
    path="${path}/**"
  fi

  # 年的头两位就不用处理了，所以到 i=2
  local len=${#dir}
  for ((i = len - 1; i >= 2; i--)); do
    local prefix=${dir:0:i}
    local char=${dir:i:1}

    ! [[ "$char" =~ [0-9] ]] && continue  

    # 将字符转为数字
    num=$((char))
    if (( num < 9 )); then
      new_num=$((num + 1))
      echo "+ ${prefix}[${new_num}-9]*/${path}" >> "$file"
    fi
  done

  echo "- **" >> "$file"
}

# 将时间段转为 UTC 时间，用指定时间减去时间段
convert_duration_to_utc(){
  local duration="$1" # 时间段，例如 "1y2M3w4d5h6m7s"
  local time_str="$2" # 时间字符串，需要带有时区信息，例如 "2023-10-01T00:00:00+08:00"

  # 时间段，使用正则表达式一次性提取所有时间单位，匹配零次或多次，包括周（w）
  if [[ "$duration" =~ ^([0-9]+y)?([0-9]+M)?([0-9]+w)?([0-9]+d)?([0-9]+h)?([0-9]+m)?([0-9]+s)?$ ]]; then
    # 提取每个部分，如果没有找到某个部分，它会是空字符串，使用默认值 0
    local years=${BASH_REMATCH[1]:-0y}; years="${years//y/}"
    local months=${BASH_REMATCH[2]:-0M}; months="${months//M/}"
    local weeks=${BASH_REMATCH[3]:-0w}; weeks="${weeks//w/}"
    local days=${BASH_REMATCH[4]:-0d}; days="${days//d/}"
    local hours=${BASH_REMATCH[5]:-0h}; hours="${hours//h/}"
    local minutes=${BASH_REMATCH[6]:-0m}; minutes="${minutes//m/}"
    local seconds=${BASH_REMATCH[7]:-0s}; seconds="${seconds//s/}"

    days=$((days + weeks * 7))
    [ -z "$time_str" ] && time_str=$(date -u "+%Y-%m-%dT%H:%M:%SZ")
    echo $(date -u -d "$time_str -$years years -$months months -$days days -$hours hours -$minutes minutes -$seconds seconds" "+%Y-%m-%dT%H:%M:%SZ")
    return 0
  else
    echo "invalid time format: $duration"
    return 1
  fi
}

# 将时间转为 UTC 时间
convert_time_to_utc(){
  local time_str="$1"
  local new_time

  # RFC3339 格式：如 2006-01-02T15:04:05+07:00
  if [[ "$time_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([+-][0-9]{2}:[0-9]{2}|Z)$ ]]; then
    new_time=$(date -u -d "$time_str" "+%Y-%m-%dT%H:%M:%SZ")

  # ISO8601 本地时区时间：如 2006-01-02T15:04:05 或 2006-01-02 15:04:05
  elif [[ "$time_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$ || "$time_str" =~ "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$" ]]; then
    local tz_offset=$(date "+%:z")
    new_time=$(date -u -d "${time_str}${tz_offset}" "+%Y-%m-%dT%H:%M:%SZ")

  # 处理仅日期：如 2006-01-02
  elif [[ "$time_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    local tz_offset=$(date "+%:z")
    new_time=$(date -u -d "${time_str}T00:00:00${tz_offset}" "+%Y-%m-%dT%H:%M:%SZ")
  else
    echo "invalid time format: $time_str"
    return 1
  fi
  echo "$new_time"
}

# 去掉开头结尾的未转义 /
trim_slash() {
  local input=$1

  while true; do
    local len=${#input}
    local orignal=$input
    # 去掉开头的未转义 /
    if [[ $input =~ ^/ ]]; then
      input=${input:1}
    fi

    # 去掉结尾的未转义 /
    if [[ $input =~ /$ ]]; then
      # 检查结尾 / 前的反斜杠是否为奇数
      local len=${#input}
      local i=$((len - 2)) # 倒数第二个字符
      local flag=false
      while [[ $i -ge 0 && ${input:i:1} == "\\" ]]; do
        flag=$([[ $flag == false ]] && echo true || echo false)
        ((i--))
      done

      # 如果 flag 为 false，说明 / 未被转义
      if [[ $flag == false ]]; then
        input=${input:0:len-1}
      fi
    fi

    [[ "$input" == "$orignal" ]] && break
  done
  echo "$input"
}

# 统计字符串中的 / 数量
count_slashes() {
  local input=$1
  local flag=false
  local count=0

  for ((i = 0; i < ${#input}; i++)); do
    char="${input:i:1}"
    if [[ $char == "\\" ]]; then
      # 遇到反斜杠，切换 flag
      flag=$([[ $flag == false ]] && echo true || echo false)
    elif [[ $char == "/" ]]; then
      if [[ $flag == false ]]; then
        # 未被转义的斜杠，统计一次
        ((count++))
        # 跳过连续的斜杠
        while [[ ${input:i+1:1} == "/" ]]; do
            ((i++))
        done
      fi
      # 斜杠后，flag重置
      flag=false
    else
      # 遇到其他字符，flag重置
      flag=false
    fi
  done

  # 返回统计结果
  echo $count
}

get_depth(){
  local input=$(trim_slash $1)

  if [[ -z "$input" ]]; then
    echo 1
  else
    local depth=$(count_slashes "$input")
    echo $((depth + 2))
  fi
}

# 初始化备份库
# 被初始化的目录，应该要保持独立性，不能跟其他备份库有交集
# 初始化时决定备份方式：全量备份还是增量备份
cmd_init(){
  is_empty_dir "$store_path" || return 1

  local type="full"
  if [ "$1" == "-i" ]; then
    type="$INCREMENTAL"
    shift
  fi
  local backup_options=("$@")

  echo "init $store_path"
  lock $store_path || return 1
  trap 'unlock "$store_path"' RETURN

  local btime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  jq -n -c --argjson opts "$(printf '%s\n' "${backup_options[@]}" | jq -R | jq -s 'map(select(length > 0))')" \
    '{"id":"'$ID_ALL'","btime":"'$btime'","etime":"","size":0,"backupType":"'$type'","backupOptions":$opts}' \
    > "$TEMP_SNAPSHOTS_FILE_NEW"

  rclone_show_cmd mkdir "${store_path}/caman-content"
  rclone_show_cmd mkdir "${store_path}/caman-backup"
  put_snapshots_file || return 1

  echo "Init successful"
}

# 删除备份库
# -f 强制删除
cmd_delete(){
  if [ "$1" != "-f" ]; then
    read -p "Are you sure you want to delete the $store_path directory? [yes/No] " reply leftover
    if [[ "$reply" != y* && "$reply" != Y* ]]; then
      echo "Deletion cancelled."
      return 0
    fi
  fi

  lock "$store_path" || return 1
  rclone_show_cmd purge "$store_path" && {
    echo "Deleted $store_path successfully"
    return 0
  }
  unlock "$store_path"
}

# 备份
# 普通备份用 sync；增量备份用 copy
# 如果第一个参数是 -c，则使用 copy 进行增量备份
# 使用 backup 命令的格式：
# camankup store_path backup [-c] /path/to/backup other_flags...
#
cmd_backup(){
  local src_path="$1"
  if [ -z "$src_path" ]; then
    show_usage_backup
    return 1
  fi
  shift 1

  lock "$store_path" || return 1
  trap 'unlock "$store_path"' RETURN
  get_snapshots_file || return 1

  local backup_type=$(jq -r -s '.[0].backupType' "$TEMP_SNAPSHOTS_FILE")
  local last_bkp_size=0
  local last_bkp_time=""
  local lines=$(jq -s '. | length' "$TEMP_SNAPSHOTS_FILE")
  if [ $lines -gt 1 ]; then
    read last_bkp_time last_bkp_size <<< $(jq -r -s '.[-1] | "\(.btime) \(.size)"' "$TEMP_SNAPSHOTS_FILE")
  fi

  local btime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  local bkp_dir=$(date -u +%Y%m%d%H%M%S)
  local new_args=($(filter_option "--backup-dir"  "$@"))
  if ! has_log_file "${new_args[@]}"; then
    new_args+=("--log-file" "$temp_dir/backup.log")
  fi

  local cmd="sync"
  local max_age=""
  if [ "$backup_type" = "$INCREMENTAL" ]; then
    cmd="copy"
    if [ "$last_bkp_time" != "" ]; then
      new_args=($(filter_option "--max-age" "${new_args[@]}"))
      max_age="--max-age $last_bkp_time"
    fi
  fi

  local backup_options
  old_IFS=$IFS
  IFS=$'\n'; read -r -d '' -a backup_options <<< "$(jq -r -s '.[0].backupOptions | join("\n")' "$TEMP_SNAPSHOTS_FILE")"
  IFS=$old_IFS

  rclone_show_cmd $cmd "$src_path" "${store_path}/caman-content" "${new_args[@]}" "${backup_options[@]}" --progress \
    --verbose --backup-dir="${store_path}/caman-backup/${bkp_dir}" $max_age || return 1

  local etime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  local uuid=$(cat /proc/sys/kernel/random/uuid)
  local size_added=0
  local size_uploaded=0

  # 从日志文件中取出备份大小
  local log_file=$(get_log_file_path "${new_args[@]}")
  if [ ! -f "$log_file" ]; then
    echo "log file not found: ${log_file}, can not get backup size"
  else
    read size_added size_uploaded <<< $(get_added_size "$log_file")
  fi

  jq -c -s '.[0].size += '$size_uploaded' | .[]' "$TEMP_SNAPSHOTS_FILE" > "$TEMP_SNAPSHOTS_FILE_NEW" || return 1
  echo '{"id":"'$uuid'","btime":"'$btime'","etime":"'$etime'","size":'$((last_bkp_size + size_added))',"backupDir":"'$bkp_dir'"}' >> "$TEMP_SNAPSHOTS_FILE_NEW"
  put_snapshots_file || return 1

  echo "Backup completed successfully"
}

# 删除备份的内容，返回减少的大小
remove_sub(){
  local snaps="$1"
  local snaps_num=$(echo "$snaps" | jq -r '. | length')
  local id0=$(echo $snaps | jq -r '.[0].id')
  local size_del=0

  # 第一种情况：
  # 只有两条，且上一条是all，这是仅有的一个备份，把所有数据删除即可
  if [ "$id0" == "$ID_ALL" ] && [ "$snaps_num" -eq 2 ]; then
    rclone_silent delete "${store_path}/caman-backup" --rmdirs || return 1
    rclone_silent delete "${store_path}/caman-content" --rmdirs || return 1
    size_del=$(echo $snaps | jq -r '.[0].size')

  # 第二种情况：
  # 被删除的备份是第一个备份，这时肯定有下一次备份，否则就属于第一种情况。
  # 这时当前备份的目录一定是空的，把当前备份和下一次备份的目录都删除即可
  elif [ "$id0" == "$ID_ALL" ]; then
    local cur_bkp_dir=${store_path}/caman-backup/$(echo "$snaps" | jq -r '.[1].backupDir')
    local next_bkp_dir=${store_path}/caman-backup/$(echo "$snaps" | jq -r '.[2].backupDir')

    g_out=$(rclone size "$next_bkp_dir" --json 2>&1)
    if [ $? -eq 0 ]; then
      size_del=$(echo "$g_out" | jq -r '.bytes')
    elif ! echo "$g_out" | grep -q "$DIR_NOT_FOUND"; then # 不是没有找到目录的错误
      echo "$g_out"
      return 1
    fi

    rclone_ignore_not_found purge "$cur_bkp_dir" || return 1
    rclone_ignore_not_found delete "$next_bkp_dir" --rmdirs || return 1

  # 第三种情况：
  # 被删除的备份是位于中间或最后的一个备份，这时需要：
  # 1. 把下一次备份的目录（next_bkp_dir）中，修改时间比上一次备份的时间（pre_bkp_time）晚的文件删掉
  # 2. 把本次备份的目录（cur_bkp_dir）中的文件移动到下一次备份的目录（next_bkp_dir）中
  # 如果被删除的备份是最后的一个备份，下一次备份的目录就是备份的主目录（caman-content）
  else
    local pre_bkp_time=$(echo "$snaps" | jq -r '.[0].btime')
    local cur_bkp_dir=${store_path}/caman-backup/$(echo "$snaps" | jq -r '.[1].backupDir')
    local next_bkp_dir="${store_path}/caman-content"
    if [ "$snaps_num" -eq 3 ]; then
      next_bkp_dir=${store_path}/caman-backup/$(echo "$snaps" | jq -r '.[2].backupDir')
    fi

    g_out=$(rclone size "$next_bkp_dir" --max-age $pre_bkp_time --json 2>&1)
    if [ $? -eq 0 ]; then
      size_del=$(echo "$g_out" | jq -r '.bytes')
      rclone_silent delete "$next_bkp_dir" --max-age $pre_bkp_time || return 1
    elif ! echo "$g_out" | grep -q "$DIR_NOT_FOUND"; then # 不是没有找到目录的错误
      echo "$g_out"
      return 1
    fi

    rclone_silent mkdir "$next_bkp_dir" || return 1  # 防止没有目录出错
    rclone_ignore_not_found move "$cur_bkp_dir" "$next_bkp_dir" || return 1
  fi
  echo $size_del
}

# 删除一个备份快照
cmd_remove(){
  local snapshot_id
  if [ -n "$1" ] && [ "$1" != "-f" ]; then
    snapshot_id="$1"
    read -p "Are you sure you want to remove the snapshot ${snapshot_id}? [yes/No] " reply leftover
    if [[ "$reply" != y* && "$reply" != Y* ]]; then
      echo "Remove snapshot cancelled"
      return 0
    fi
  else
    snapshot_id="$2"
  fi

  is_snapshot_empty "$snapshot_id" "show_usage_remove" || return 1

  lock "$store_path" || return 1
  trap 'unlock "$store_path"' RETURN

  get_snapshots_file || return 1
  is_snapshot_exist "$snapshot_id" || return 1
  snapshot_id=$(get_whole_snapshot_id "$snapshot_id")

  echo "remove snapshot $snapshot_id"
  local snaps=$(jq -s '. as $input | to_entries | map(select(.value.id == "'$snapshot_id'").key) | 
    first as $index | [$input[$index-1], $input[$index], $input[$index+1]] | map(select(. != null))
  ' "$TEMP_SNAPSHOTS_FILE")

  local size_del=0
  size_del=$(remove_sub "$snaps") || return 1
  jq -c -s '.[0].size -= '$size_del' | map(select(.id != "'$snapshot_id'")) |
    .[]' "$TEMP_SNAPSHOTS_FILE" > "$TEMP_SNAPSHOTS_FILE_NEW" || return 1
  put_snapshots_file || return 1

  echo "Removed snapshot $snapshot_id"
}

# 删除所有备份内容（所有快照）中某个时间点或某段时间之前的内容
cmd_rm_before(){
  local del_time=$1
  if [ -n "$1" ] && [ "$1" != "-f" ]; then
    del_time="$1"
    read -p "Are you sure you want to remove the content before ${del_time}? [yes/No] " reply leftover
    if [[ "$reply" != y* && "$reply" != Y* ]]; then
      echo "Remove content cancelled"
      return 0
    fi
  else
    del_time="$2"
  fi

  if [ -z "$del_time" ]; then
    echo "Please specify the time to delete the content before"
    show_usage_rm_before
    return 1
  fi

  lock "$store_path" || return 1
  trap 'unlock "$store_path"' RETURN

  get_snapshots_file || return 1

  local type
  type=$(jq -s -r '.[0].backupType' "$TEMP_SNAPSHOTS_FILE") || return 1
  if [ "$type" != "$INCREMENTAL" ]; then
    echo "This command is only available for incremental backups"
    return 1
  fi

  if [[ "$del_time" =~ -.*- ]]; then
    del_time=$(convert_time_to_utc "$del_time")
  else
    del_time=$(convert_duration_to_utc "$del_time")
  fi
  if [ $? -ne 0 ]; then
    echo $del_time
    return 1
  fi

  rclone delete "${store_path}/caman-content" --min-age $del_time || return 1
  rclone rmdirs "${store_path}/caman-content" --leave-root || return 1
  rclone delete "${store_path}/caman-backup" --min-age $del_time || return 1
  rclone rmdirs "${store_path}/caman-backup" --leave-root || return 1

  # 删除快照文件中所有时间点早于del_time的快照
  jq -c -s '. | map(select(.etime >= "'$del_time'" or .id == "'$ID_ALL'")) |
    .[]' "$TEMP_SNAPSHOTS_FILE" > "$TEMP_SNAPSHOTS_FILE_NEW" || return 1
  put_snapshots_file || return 1

  local local_time=$(date -d "$del_time" +"%Y-%m-%d %H:%M:%S")
  echo "Removed content before ${local_time}"
}

# 按保留策略删除备份
cmd_forget(){
  # 策略选项
  local keep_last=0
  local keep_hourly=0
  local keep_daily=0
  local keep_weekly=0
  local keep_monthly=0
  local keep_yearly=0
  local keep_within=""
  local keep_within_hourly=""
  local keep_within_daily=""
  local keep_within_weekly=""
  local keep_within_monthly=""
  local keep_within_yearly=""
  local has_policy=false

  # 解析命令行参数
  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
      -l|--keep-last)
      keep_last="$2"
      ;;
      -H|--keep-hourly)
      keep_hourly="$2"
      ;;
      -d|--keep-daily)
      keep_daily="$2"
      ;;
      -w|--keep-weekly)
      keep_weekly="$2"
      ;;
      -m|--keep-monthly)
      keep_monthly="$2"
      ;;
      -y|--keep-yearly)
      keep_yearly="$2"
      ;;
      --keep-within)
      keep_within="$2"
      ;;
      --keep-within-hourly)
      keep_within_hourly="$2"
      ;;
      --keep-within-daily)
      keep_within_daily="$2"
      ;;
      --keep-within-weekly)
      keep_within_weekly="$2"
      ;;
      --keep-within-monthly)
      keep_within_monthly="$2"
      ;;
      --keep-within-yearly)
      keep_within_yearly="$2"
      ;;
      *)    # 未知选项
      echo "unknown option: $1"
      return 1
      ;;
    esac

    if [ -n "$2" ]; then
      has_policy=true
    else
      echo "option $1 requires an argument"
      return 1
    fi

    shift 2
  done

  [ "$has_policy" == false ] && { echo "no policy specified"; return 1; }

  [[ "$keep_last" =~ ^[0-9]+$ ]] || { echo "keep_last must be a number"; return 1; }
  [[ "$keep_hourly" =~ ^[0-9]+$ ]] || { echo "keep_hourly must be a number"; return 1; }
  [[ "$keep_daily" =~ ^[0-9]+$ ]] || { echo "keep_daily must be a number"; return 1; }
  [[ "$keep_weekly" =~ ^[0-9]+$ ]] || { echo "keep_weekly must be a number"; return 1; }
  [[ "$keep_monthly" =~ ^[0-9]+$ ]] || { echo "keep_monthly must be a number"; return 1; }
  [[ "$keep_yearly" =~ ^[0-9]+$ ]] || { echo "keep_yearly must be a number"; return 1; }

  lock "$store_path" || return 1
  trap 'unlock "$store_path"' RETURN

  get_snapshots_file || return 1

  local last_bkp_time
  last_bkp_time=$(jq -r -s '.[-1].btime' "$TEMP_SNAPSHOTS_FILE") || return 1
  [ -n "$keep_within" ] && (keep_within=$(convert_duration_to_utc "$keep_within" "$last_bkp_time") || return 1)
  [ -n "$keep_within_hourly" ] && (keep_within_hourly=$(convert_duration_to_utc "$keep_within_hourly" "$last_bkp_time") || return 1)
  [ -n "$keep_within_daily" ] && (keep_within_daily=$(convert_duration_to_utc "$keep_within_daily" "$last_bkp_time") || return 1)
  [ -n "$keep_within_weekly" ] && (keep_within_weekly=$(convert_duration_to_utc "$keep_within_weekly" "$last_bkp_time") || return 1)
  [ -n "$keep_within_monthly" ] && (keep_within_monthly=$(convert_duration_to_utc "$keep_within_monthly" "$last_bkp_time") || return 1)
  [ -n "$keep_within_yearly" ] && (keep_within_yearly=$(convert_duration_to_utc "$keep_within_yearly" "$last_bkp_time") || return 1)

  # 需要删除的快照 ID 数组
  local delete_snaps=()
  local last_time_hourly=""
  local last_time_daily=""
  local last_time_weekly=""
  local last_time_monthly=""
  local last_time_yearly=""

  declare -a snapshots=($(jq -s -c '. | reverse | .[]' "$TEMP_SNAPSHOTS_FILE"))
  for snapshot in "${snapshots[@]}"; do
    [ "$(echo "$snapshot" | jq -r '.id')" == "all" ] && continue
    
    local is_new_hourly=false
    local is_new_daily=false
    local is_new_weekly=false
    local is_new_monthly=false
    local is_new_yearly=false
    local btime=$(echo "$snapshot" | jq -r '.btime')
    local local_time=$(date -d "$btime" "+%Y-%m-%dT%H:%M:%S")

    # 检查是否是一个新的时、天、周、月、年备份
    # 一个备份是哪一时、天、周、月、年的备份，是按本地时间来计算的
    [[ "$last_time_hourly" != "${local_time:0:13}" ]] && { last_time_hourly="${local_time:0:13}"; is_new_hourly=true; }
    [[ "$last_time_daily" != "${local_time:0:10}" ]] && { last_time_daily="${local_time:0:10}"; is_new_daily=true; }
    [[ "$last_time_weekly" != "${local_time:0:10}" && $(date -d "$last_time_weekly" +%W) -ne $(date -d "$local_time" +%W) ]] && { last_time_weekly="${local_time:0:10}"; is_new_weekly=true; }
    [[ "$last_time_monthly" != "${local_time:0:7}" ]] && { last_time_monthly="${local_time:0:7}"; is_new_monthly=true; }
    [[ "$last_time_yearly" != "${local_time:0:4}" ]] && { last_time_yearly="${local_time:0:4}"; is_new_yearly=true; }

    # 检查是否需要删除。这里比较时可以不用本地时间，只要所比较的两个时间的时区相同即可
    if [[ $keep_last -gt 0 || ("$keep_within" != "" && "$btime" > "$keep_within") ]]; then
      keep_last=$((keep_last - 1))
    elif [[ ($keep_hourly -gt 0 || ("$keep_within_hourly" != "" && "$btime" > "$keep_within_hourly")) && $is_new_hourly == true ]]; then
      keep_hourly=$((keep_hourly - 1))
    elif [[ ($keep_daily -gt 0 || ("$keep_within_daily" != "" && "$btime" > "$keep_within_daily")) && $is_new_daily == true ]]; then
      keep_daily=$((keep_daily - 1))
    elif [[ ($keep_weekly -gt 0 || ("$keep_within_weekly" != "" && "$btime" > "$keep_within_weekly")) && $is_new_weekly == true ]]; then
      keep_weekly=$((keep_weekly - 1))
    elif [[ ($keep_monthly -gt 0 || ("$keep_within_monthly" != "" && "$btime" > "$keep_within_monthly")) && $is_new_monthly == true ]]; then
      keep_monthly=$((keep_monthly - 1))
    elif [[ ($keep_yearly -gt 0 || ("$keep_within_yearly" != "" && "$btime" > "$keep_within_yearly")) && $is_new_yearly == true ]]; then
      keep_yearly=$((keep_yearly - 1))
    else
      delete_snaps+=("$snapshot")
    fi
  done

  # 逐个删除快照
  local deleted_success=()
  local deleted_failed=()
  local deleted_ids=()
  local size_del_all=0
  for snap in "${delete_snaps[@]}"; do
    local snapshot_id=$(echo "$snap" | jq -r '.id')

    local snaps=$(jq -s '. as $input | to_entries | map(select(.value.id == "'$snapshot_id'").key) | 
      first as $index | [$input[$index-1], $input[$index], $input[$index+1]] | map(select(. != null))
    ' "$TEMP_SNAPSHOTS_FILE")

    local size_del=0
    size_del=$(remove_sub "$snaps")
    if [[ $? -eq 0 ]]; then
      size_del_all=$((size_del_all + size_del))
      deleted_success+=("$snap")
      deleted_ids+=("$snapshot_id")
      echo "Removed snapshot $snapshot_id"
    else
      deleted_failed+=("$snap")
      echo "Failed to removed snapshot $snapshot_id"
    fi
  done

  jq --argjson ids "$(printf '%s\n' "${deleted_ids[@]}" | jq -R | jq -s .)" -c -s '.[0].size -= '$size_del_all' |
    map(select(.id | IN($ids[]) | not)) | .[]' "$TEMP_SNAPSHOTS_FILE" > "$TEMP_SNAPSHOTS_FILE_NEW" || return 1
  put_snapshots_file || return 1

  echo "Deleted ${#deleted_ids[@]} snapshots successfully, ${#deleted_failed[@]} failed"
  # echo ${deleted_success[@]} | jq -s
}

# 浏览快照内容，本次备份快照的内容包括：
# 1. caman-content 下，修改时间在 cur_bkp_time 之前的文件。修改时间在这之后的，肯定不会出现在本次备份中。
# 2. caman-backup 下，在以后备份的目录中，修改时间在 cur_bkp_time 之前的文件。因为：
#    1) 修改时间在这之后的，肯定不会出现在本次备份中；
#    2) 在本次备份及以前备份的目录中的文件，都是在做本次备份时，已经被覆盖或删除的，所以不会出现在本次备份中。
cmd_list(){
  local snapshot_id="$1"
  local path=$(trim_slash "$2")

  is_snapshot_empty "$snapshot_id" "show_usage_list" || return 1

  get_snapshots_file || return 1
  is_snapshot_exist "$snapshot_id" || return 1
  snapshot_id=$(get_whole_snapshot_id "$snapshot_id")

  local depth=$(get_depth "$path")
  local snap=$(jq -s '.[] | select(.id | startswith("'$snapshot_id'"))' "$TEMP_SNAPSHOTS_FILE")
  local cur_bkp_time=$(echo "$snap" | jq -r '.etime')
  local cur_bkp_dir=$(echo "$snap" | jq -r '.backupDir')
  local filter_file="${temp_dir}/filter_file"

  gen_filter_file "$cur_bkp_dir" "$filter_file" "$path"

  # 只将 path 下的对象过滤出来
  local pre_path=$path
  if [ "$pre_path" != "" ]; then
    pre_path="${pre_path}/"
  fi
  rclone lsjson "${store_path}/caman-content" -R --max-depth $depth --min-age $cur_bkp_time --no-mimetype \
    --filter "+ ${path}/**" --filter "- **" | jq '[.[] | select(.Path | startswith("'${pre_path}'"))]' \
    > "${temp_dir}/list1.json"
  rclone lsjson "${store_path}/caman-backup" -R --max-depth $((depth + 1)) --min-age $cur_bkp_time --no-mimetype \
    --filter-from "$filter_file" | jq '[.[] | select(.Path | test("^[0-9]{14}/'${pre_path}'"))]' \
    > "${temp_dir}/list2.json"
  jq -s 'map(.[]) | unique_by(.Name) | sort_by(.Name)' "${temp_dir}/list1.json" "${temp_dir}/list2.json"
}

# 恢复快照
cmd_restore(){
  local snapshot_id="$1"
  local backup_path="$2"     # 被恢复的路径，必须有，可以是 /
  local restore_path="$3"    # 恢复到哪个路径，必须有

  is_snapshot_empty "$snapshot_id" "show_usage_restore" || return 1

  if [ -z "$backup_path" ] || [ -z "$restore_path" ]; then
    show_usage_restore
    return 1
  fi

  get_snapshots_file || return 1
  is_snapshot_exist "$snapshot_id" || return 1
  snapshot_id=$(get_whole_snapshot_id "$snapshot_id")

  local snap=$(jq -s '.[] | select(.id | startswith("'$snapshot_id'"))' "$TEMP_SNAPSHOTS_FILE")
  local cur_bkp_time=$(echo "$snap" | jq -r '.etime')
  local cur_bkp_dir=$(echo "$snap" | jq -r '.backupDir')
  local filter_file="${temp_dir}/filter_file"

  backup_path=$(trim_slash "$backup_path")
  gen_filter_file "$cur_bkp_dir" "$filter_file" "$backup_path"

  rclone copy "${store_path}/caman-content" "$restore_path" --min-age $cur_bkp_time --filter "+ ${backup_path}/**" --filter "- **"

  # 如果符合条件的目录比较多，这时性能比较低下
  rclone lsf "${store_path}/caman-backup" --dirs-only --min-age $cur_bkp_time --filter-from "$filter_file" | while read dir; do
    rclone copy "${store_path}/caman-backup/${dir}" "$restore_path"
  done
}

# 列出快照列表
cmd_snapshots(){
  get_snapshots_file || return 1
  jq -s '.[1:]' "$TEMP_SNAPSHOTS_FILE"
}

# 查询快照信息
cmd_snapshot(){
  local snapshot_id="$1"
  is_snapshot_empty "$snapshot_id" "show_usage_snapshot" || return 1

  get_snapshots_file || return 1
  g_out=$(jq -s '.[] | select(.id | startswith("'$snapshot_id'"))' "$TEMP_SNAPSHOTS_FILE")
  if [ $? -ne 0 ]; then
    echo $g_out
    return 1
  elif [ -z "$g_out" ]; then
    echo "Snapshot $snapshot_id not found"
    return 1
  fi

  echo $g_out | jq 
}

# 查询库的大小
cmd_size(){
  get_snapshots_file || return 1
  jq -s -c '{bytes: .[0].size}' "$TEMP_SNAPSHOTS_FILE"
}

# 查询库的信息
cmd_info(){
  get_snapshots_file || return 1
  jq -s '.[0] + {"snapshotCount": (length - 1)}' "$TEMP_SNAPSHOTS_FILE"
}

# 手动解除未能正常解的锁
cmd_rmlock(){
  if [ "$1" != "-f" ]; then
    read -p "Are you sure you want to remove the lock on ${store_path}? [yes/No] " reply leftover
    if [[ "$reply" != y* && "$reply" != Y* ]]; then
      echo "Remove lock cancelled"
      return 0
    fi
  fi
  unlock || return 1
  echo "Lock removed"
}

# 显示各命令的帮助信息
cmd_help(){
  local cmd="$1"
  if [ -z "$cmd" ]; then
    show_usage
  elif [[ " ${CMD_LIST[@]} " =~ " $cmd " ]]; then
    show_usage_$cmd
  else
    unknown_cmd "$cmd"
    return 1
  fi
}

# 未知操作
unknown_cmd(){
  echo "Unknown command: $1"
  echo
  show_usage
}

# 主脚本入口
if [ "$1" == "help" ]; then
  cmd="$2"
  cmd_help $cmd
  exit 0
fi

store_path="$1"
cmd="$2"
shift 2

if [ -z "$cmd" ] || [ -z "$store_path" ]; then
  cmd_help 
  exit 1
fi

if [[ ! " ${CMD_LIST[@]} " =~ " $cmd " ]]; then
  unknown_cmd "$cmd"
  exit 1
fi

if [ "$cmd" != "init" ] && ! is_backup "$store_path"; then
    echo "Not a backup repository: $store_path"
    exit 1
fi

temp_dir=$(mktemp -d --suffix=.caman)
trap 'rm -rf "$temp_dir"' EXIT

STORE_SNAPSHOTS_FILE=${store_path}/caman-meta/snapshots
TEMP_SNAPSHOTS_FILE=${temp_dir}/snapshots
TEMP_SNAPSHOTS_FILE_NEW=${temp_dir}/snapshots.new

if ! cmd_$cmd "$@"; then
  echo "Failed to execute command: $cmd"
  exit 1
fi
