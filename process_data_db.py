import json
import feedparser
import os
import sqlite3
from bs4 import BeautifulSoup

#Read json file get url
with open("data/news_site.json", 'r', encoding='utf-8') as f:
    data =json.load(f)
rss_sources = data['rss_sources']

db_directory = 'data'
db_file = 'news_db.db'
db_path = os.path.join(db_directory, db_file)

def fetch_rss():
    # Connect Sqlite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    total_articles = 0

    for source, categories in rss_sources.items():
        for category, url in categories.items():
            feed = feedparser.parse(url)

            if feed.bozo == 0:
                for entry in feed.entries:
                    soup = BeautifulSoup(entry.description, 'html.parser')
                    description_text = soup.get_text()

                    # Kiểm tra xem bài viết đã tồn tại trong cơ sở dữ liệu chưa
                    cursor.execute("SELECT COUNT(*) FROM news_articles WHERE link = ?", (entry.link,))
                    exists = cursor.fetchone()[0]

                    if exists == 0:
                        published_clean = entry.published.split(' +')[0] if ' +' in entry.published else entry.published
                        cursor.execute('''
                            INSERT OR IGNORE INTO news_articles (title, link, description, published, category, source)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (entry.title, entry.link, description_text, published_clean, category, source))
                        total_articles += 1
            else:
                print(f"Có lỗi khi tải RSS feed từ {url}của {source}")


    conn.commit()
    conn.close()

    print(f"Đã lưu {total_articles} bài viết vào cơ sở dữ liệu {db_path}")

def delete_outdate_news():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    DELETE FROM news_articles
    WHERE datetime(
            substr(published, 13, 4) || '-' ||  -- Lấy năm
            CASE substr(published, 9, 3)        -- Lấy tháng và chuyển đổi sang dạng số
                WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
            END || '-' || 
            substr(published, 6, 2) || ' ' ||   -- Lấy ngày
            substr(published, 18, 8)            -- Lấy giờ, phút, giây
        ) < datetime('now', '-2 hours')
    ''')
    
    del_count = cursor.rowcount
    print(f"Số bài viết đã xóa: {del_count}")
    
    conn.commit()  # Lưu thay đổi
    conn.close()  # Đóng kết nối

