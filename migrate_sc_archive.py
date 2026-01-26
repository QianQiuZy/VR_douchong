"""Super Chat 历史数据归档脚本。

用法：
  python migrate_sc_archive.py           # 归档所有早于当前月的数据
  python migrate_sc_archive.py --month 202401  # 归档指定月份
"""

import argparse
import logging

from gift import archive_super_chat_log, normalize_month_code


def main() -> None:
    parser = argparse.ArgumentParser(description="归档 super_chat_log 历史数据")
    parser.add_argument("--month", help="指定月份（YYYYMM 或 YYYY-MM）")
    args = parser.parse_args()

    target_month = None
    if args.month:
        target_month = normalize_month_code(args.month)
        if not target_month:
            raise SystemExit("month 参数格式不正确，应为 YYYYMM 或 YYYY-MM")

    moved = archive_super_chat_log(target_month)
    logging.info("[migrate_sc_archive] 归档完成，移动记录数 ~%s", moved)


if __name__ == "__main__":
    main()
