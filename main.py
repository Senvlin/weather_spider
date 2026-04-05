import argparse
import csv
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import httpx
import parsel


def get_download_dir():
    try:
        from winreg import HKEY_CURRENT_USER, OpenKey, QueryValueEx

        key = OpenKey(
            HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders",
        )
        download_path, _ = QueryValueEx(key, "{374DE290-123F-4565-9164-39C4925E467B}")
        return Path(download_path)
    except Exception:
        # 如果读取注册表失败，回退到默认路径
        return Path.home() / "Downloads"


EXPORT_DIR = get_download_dir()


@dataclass
class Item: ...


@dataclass
class InfoItem(Item):
    date: str
    sunrise_time: str
    sunset_time: str
    AQI: int
    total_shortwave: str


class Parser(ABC):
    def __init__(self, html_str):
        self._selector = parsel.Selector(text=html_str)

    @abstractmethod
    def parse(self) -> Generator[Item, None, None]:
        """在这里写逻辑,返回解析数据"""
        ...


class InfoParser(Parser):
    def __init__(self, html_str):
        super().__init__(html_str)

    def parse(self) -> Generator[InfoItem, None, None]:
        table = self._selector.css("table")[1]
        ths = table.css("thead>tr>th")
        AQI_index, sr_ss_index, sw_index = self._get_indexs(ths)
        trs = table.css("tbody>tr")
        yield from self._get_infos(AQI_index, sr_ss_index, sw_index, trs)

    def _get_infos(
        self, AQI_index, sr_ss_index, sw_index, trs
    ) -> Generator[InfoItem, None, None]:
        for tr in trs:
            td = tr.css("td")
            date = td[0].css("::text").get()
            times = td[sr_ss_index].css("::text").get()
            sunrise_time, sunset_time = times.split("/")
            AQI = td[AQI_index].css("::text").get()
            total_shortwave = td[sw_index].css("::text").get()
            if total_shortwave != "-":
                total_shortwave += "MJ/m²"
            else:
                total_shortwave = ""
            yield InfoItem(date, sunrise_time, sunset_time, AQI, total_shortwave)

    def _get_indexs(self, ths) -> tuple[int, int, int]:
        AQI_index = None
        sr_ss_index = None
        sw_index = None
        for index, th in enumerate(ths):
            th_text = th.css("::text").get().strip()
            if th_text == "空气质量AQI":
                AQI_index = index
            elif th_text == "日出/日落":
                sr_ss_index = index
            elif th_text == "短波辐射总量(MJ/m²)":
                sw_index = index
        if AQI_index is None or sr_ss_index is None or sw_index is None:
            raise ValueError("未能找到所有必需的表头")

        return (AQI_index, sr_ss_index, sw_index)


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
}


def grab_data():

    req = httpx.get(
        "https://datashareclub.com/weather/%E5%90%89%E6%9E%97/%E5%BB%B6%E8%BE%B9/101060308.html",
        headers=headers,
    )
    html = req.text
    info_parser = InfoParser(html)
    with sqlite3.connect("weather_data.db") as conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS weather_data
                  (ID INTEGER PRIMARY KEY AUTOINCREMENT,
                   date TEXT NOT NULL UNIQUE,
                   sunrise_time TEXT,
                   sunset_time TEXT,
                   AQI INTEGER,
                   total_shortwave TEXT)""")
        for info in info_parser.parse():
            c.execute(
                """
                INSERT INTO weather_data (date,sunrise_time,sunset_time,AQI,total_shortwave) VALUES (?,?,?,?,?)
                ON CONFLICT(date)
                DO UPDATE SET
                    sunrise_time = excluded.sunrise_time,
                    sunset_time = excluded.sunset_time,
                    AQI = excluded.AQI,
                    total_shortwave = excluded.total_shortwave
                """,
                (
                    info.date,
                    info.sunrise_time,
                    info.sunset_time,
                    info.AQI,
                    info.total_shortwave,
                ),
            )


def export_data():
    with sqlite3.connect("weather_data.db") as conn:
        c = conn.cursor()
        curser = c.execute(
            """SELECT date,sunrise_time,sunset_time,AQI,total_shortwave FROM weather_data"""
        )
        with open(
            f"{EXPORT_DIR}/weather_data.csv", "w", encoding="utf-8", newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                ["日期", "日出时间", "日落时间", "空气质量AQI", "短波辐射总量"]
            )
            for row in curser:
                processed_row = ("NaN" if not value else value for value in row)
                writer.writerow(processed_row)


if __name__ == "__main__":
    # 1. 创建解析器
    parser = argparse.ArgumentParser(description="我的数据处理工具")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    parser_import = subparsers.add_parser("import", help="抓取数据")
    parser_export = subparsers.add_parser("export", help="导出csv数据到下载目录")
    args = parser.parse_args()

    if args.command == "import":
        print("正在爬取数据")
        grab_data()
        print("爬取完成，已存储到数据库中")
    elif args.command == "export":
        print("正在导出")
        export_data()
        print("导出成功，已导出为csv文件")
    else:
        parser.print_help()
