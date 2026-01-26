"""历史数据归档脚本（SC / Live Session / Room Live Stats）。

用法：
  python migrate_sc_archive.py           # 归档所有早于当前月的数据
  python migrate_sc_archive.py --month 202401  # 归档指定月份
"""

import argparse
import logging

from gift import (
    archive_live_session,
    archive_room_live_stats,
    archive_super_chat_log,
    normalize_month_code,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="归档 super_chat_log 历史数据")
    parser.add_argument("--month", help="指定月份（YYYYMM 或 YYYY-MM）")
    args = parser.parse_args()

    target_month = None
    if args.month:
        target_month = normalize_month_code(args.month)
        if not target_month:
            raise SystemExit("month 参数格式不正确，应为 YYYYMM 或 YYYY-MM")

    moved_sc = archive_super_chat_log(target_month)
    moved_live_session = archive_live_session(target_month)
    moved_room_live = archive_room_live_stats(target_month)
    logging.info(
        "[migrate_sc_archive] 归档完成，SC ~%s，LiveSession ~%s，RoomLiveStats ~%s",
        moved_sc,
        moved_live_session,
        moved_room_live,
    )


if __name__ == "__main__":
    main()
