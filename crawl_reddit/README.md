## Mục Lục

1. [Tổng quan](#tổng-quan)
2. [Cấu trúc thư mục](#cấu-trúc-thư-mục)
3. [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
4. [Cài đặt](#cài-đặt)
5. [Cách chạy chương trình](#cách-chạy-chương-trình)
   - [Sử dụng cơ bản](#sử-dụng-cơ-bản)
   - [Thu thập theo chủ đề](#thu-thập-theo-chủ-đề)
   - [Thu thập nhiều subreddit](#thu-thập-nhiều-subreddit)
   - [Tìm kiếm theo từ khóa](#tìm-kiếm-theo-từ-khóa)
   - [Chế độ OAuth (tốc độ cao hơn)](#chế-độ-oauth-tốc-độ-cao-hơn)
   - [Chế độ nhanh / nhẹ](#chế-độ-nhanh--nhẹ)
   - [Toàn bộ tùy chọn CLI](#toàn-bộ-tùy-chọn-cli)
6. [Giải thích các file đầu ra](#giải-thích-các-file-đầu-ra)
   - [reddit_big_data.json](#reddit_big_datajson)
   - [reddit_big_data.jsonl](#reddit_big_datajsonl)
   - [crawler.log](#crawlerlog)
   - [Cấu trúc dữ liệu đầu ra](#cấu-trúc-dữ-liệu-đầu-ra)
7. [Hiểu kết quả hiển thị trên màn hình](#hiểu-kết-quả-hiển-thị-trên-màn-hình)
8. [Giới hạn tốc độ & Lưu ý khi sử dụng](#giới-hạn-tốc-độ--lưu-ý-khi-sử-dụng)
9. [Xử lý sự cố](#xử-lý-sự-cố)

---

## Tổng Quan

`reddit_big_crawl.py` là script Python thu thập bài đăng Reddit với quy mô lớn (mặc định ~2.000 bài) và lưu **metadata** cho từng bài, bao gồm:

- ✅ Nội dung bài đăng (tiêu đề, nội dung, URL, flair, giải thưởng, v.v.)
- ✅ Chỉ số tương tác (điểm, tỉ lệ upvote, số bình luận, số lần crosspost)
- ✅ Bình luận lồng nhau (tối đa 3 cấp độ, tối đa 50 bình luận/bài)
- ✅ Hồ sơ tác giả (karma, tuổi tài khoản, huy hiệu)
- ✅ Thông tin subreddit

Dữ liệu thu thập được lưu dưới cả hai định dạng **JSON** và **JSONL**, phù hợp cho các pipeline xử lý dữ liệu lớn.

---

## Cấu Trúc Thư Mục

```
IT4931/
├── reddit_big_crawl.py      # Script thu thập dữ liệu chính
├── analyze_data.py          # Tiện ích phân tích dữ liệu
├── requirements.txt         # Các thư viện Python cần thiết
├── reddit_big_data.json     # Đầu ra: toàn bộ dữ liệu (mảng JSON)
├── reddit_big_data.jsonl    # Đầu ra: JSON phân dòng (streaming)
├── crawler.log              # File log theo dõi quá trình thu thập
└── README.md                # File này
```

---

## Yêu Cầu Hệ Thống

- **Python 3.10 trở lên** 
- Kết nối Internet có thể truy cập `reddit.com`
- Tài khoản Reddit *(tùy chọn — chỉ cần nếu dùng OAuth / tốc độ cao hơn)*

---

## Cài Đặt

**1. Clone repository**

```bash
git clone <đường-dẫn-repo>
cd IT4931
```

**2. Tạo và kích hoạt môi trường ảo** *(khuyến nghị)*

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows
```

**3. Cài đặt các thư viện cần thiết**

```bash
pip install -r requirements.txt
```

Thư viện bên ngoài duy nhất cần là `requests>=2.31.0` và `urllib3>=2.0.0`.

---

## Cách Chạy Chương Trình

### Sử Dụng Cơ Bản

Thu thập **2.000 bài** từ một subreddit đơn (không cần đăng nhập):

```bash
python reddit_big_crawl.py --all-topics --posts-per-topic 1000
```

---

### Thu Thập Theo Chủ Đề

Script hỗ trợ **10 chủ đề có sẵn**. Mỗi chủ đề tự động ánh xạ tới danh sách các subreddit liên quan:

| Chủ đề | Các subreddit bao gồm |
|---|---|
| `sports` | sports, nba, nfl, soccer, formula1, tennis, baseball, hockey, mma, athletics |
| `finance` | finance, investing, personalfinance, wallstreetbets, stocks, economy, cryptocurrency, financialindependence |
| `technology` | technology, programming, MachineLearning, compsci, cybersecurity, datascience, devops |
| `trending` | all, popular, worldnews, todayilearned, explainlikeimfive, askreddit, mildlyinteresting |
| `science` | science, askscience, physics, chemistry, biology, space, EarthPorn, geology |
| `health` | health, medicine, mentalhealth, fitness, nutrition, loseit, running, bodyweightfitness |
| `politics` | politics, worldnews, news, geopolitics, PoliticalDiscussion, europe, uknews |
| `entertainment` | movies, television, Music, gaming, books, anime, comicbooks, hiphopheads |
| `business` | business, entrepreneur, startups, smallbusiness, marketing, ecommerce, sales |
| `education` | learnprogramming, languagelearning, studytips, college, math, AskAcademia, GradSchool |

```bash
# Thu thập 2000 bài về thể thao
python reddit_big_crawl.py --topic sports --posts 2000

# Thu thập các bài tài chính xếp hạng cao nhất trong tuần
python reddit_big_crawl.py --topic finance --posts 2000 --sort top --time week

# Thu thập bài trending nhanh (bỏ qua bình luận)
python reddit_big_crawl.py --topic trending --posts 500 --no-comments
```

> **Lưu ý:** Khi dùng `--topic`, file đầu ra tự động đặt tên theo chủ đề (ví dụ: `reddit_sports.json`, `reddit_sports.jsonl`) trừ khi bạn ghi đè bằng `--output` / `--jsonl`.

---

### Thu Thập Nhiều Subreddit

```bash
python reddit_big_crawl.py --subreddits worldnews,news,technology --posts 2000
```

Trình thu thập sẽ xoay vòng qua các subreddit cho đến khi đạt đủ số bài mục tiêu.

---

### Tìm Kiếm Theo Từ Khóa

Tìm kiếm toàn Reddit theo từ khóa cụ thể:

```bash
python reddit_big_crawl.py --keyword "trí tuệ nhân tạo" --posts 2000
python reddit_big_crawl.py --keyword "climate change" --posts 1000 --sort top --time month
```

> Khi dùng `--keyword`, `--subreddit` / `--subreddits` sẽ bị bỏ qua (tìm kiếm toàn cục).

---

### Chế Độ OAuth (Tốc Độ Cao Hơn)

Không có OAuth, script chờ **2,5 giây** giữa mỗi request (giới hạn của Reddit cho người dùng ẩn danh).  
Với OAuth, độ trễ giảm xuống còn **0,6 giây** — nhanh hơn ~4 lần.

**Các bước để lấy thông tin xác thực OAuth:**

1. Truy cập <https://www.reddit.com/prefs/apps>
2. Nhấn **"Create App"** → chọn **"script"**
3. Nhập bất kỳ redirect URI nào (ví dụ: `http://localhost`)
4. Sao chép **client ID** (hiển thị bên dưới tên app) và **client secret**

**Chạy với OAuth:**

```bash
python reddit_big_crawl.py \
    --subreddit science \
    --posts 2000 \
    --sort top --time week \
    --client-id CLIENT_ID_CỦA_BẠN \
    --client-secret CLIENT_SECRET_CỦA_BẠN \
    --reddit-user TÊN_NGƯỜI_DÙNG \
    --reddit-pass MẬT_KHẨU
```

**Hoặc đặt thông tin xác thực qua biến môi trường** (khuyến nghị — tránh lộ thông tin trong lịch sử lệnh):

```bash
export REDDIT_CLIENT_ID=CLIENT_ID_CỦA_BẠN
export REDDIT_CLIENT_SECRET=CLIENT_SECRET_CỦA_BẠN
export REDDIT_USERNAME=TÊN_NGƯỜI_DÙNG
export REDDIT_PASSWORD=MẬT_KHẨU

python reddit_big_crawl.py --subreddit science --posts 2000
```

---

### Chế Độ Nhanh / Nhẹ

Bỏ qua việc lấy bình luận và/hoặc hồ sơ người dùng để tăng tốc đáng kể:

```bash
# Bỏ qua cả bình luận và hồ sơ người dùng
python reddit_big_crawl.py --subreddit worldnews --posts 2000 --no-comments --no-users

# Chỉ bỏ qua hồ sơ người dùng
python reddit_big_crawl.py --subreddit worldnews --posts 2000 --no-users
```

---

### Toàn Bộ Tùy Chọn CLI

```
usage: reddit_big_crawl.py [-h]
                           [--topic CHỦ_ĐỀ]
                           [--subreddit SUBREDDIT]
                           [--subreddits SUBREDDITS]
                           [--keyword TỪ_KHÓA]
                           [--posts SỐ_BÀI]
                           [--sort {new,hot,top,controversial,rising}]
                           [--time {hour,day,week,month,year,all}]
                           [--max-comments SỐ_BL_TỐI_ĐA]
                           [--min-comments SỐ_BL_TỐI_THIỂU]
                           [--no-comments]
                           [--no-users]
                           [--output FILE_JSON]
                           [--jsonl FILE_JSONL]
                           [--client-id CLIENT_ID]
                           [--client-secret CLIENT_SECRET]
                           [--reddit-user TÊN_NGƯỜI_DÙNG]
                           [--reddit-pass MẬT_KHẨU]
```

| Tham số | Mặc định | Mô tả |
|---|---|---|
| `--topic` | *(không có)* | Thu thập theo chủ đề (tự chọn subreddit phù hợp) |
| `--subreddit` | `worldnews` | Một subreddit duy nhất để thu thập |
| `--subreddits` | *(không có)* | Danh sách subreddit cách nhau bằng dấu phẩy |
| `--keyword` | *(không có)* | Tìm kiếm toàn Reddit theo từ khóa (bỏ qua subreddit) |
| `--posts` | `2000` | Số bài mục tiêu cần thu thập |
| `--sort` | `new` | Thứ tự sắp xếp: `new`, `hot`, `top`, `controversial`, `rising` |
| `--time` | `all` | Bộ lọc thời gian cho `top`/`controversial`: `hour`, `day`, `week`, `month`, `year`, `all` |
| `--max-comments` | `50` | Số bình luận tối đa lấy mỗi bài |
| `--min-comments` | `15` | Bỏ qua bài có ít hơn N bình luận |
| `--no-comments` | `False` | Bỏ qua việc lấy bình luận |
| `--no-users` | `False` | Bỏ qua việc lấy hồ sơ tác giả |
| `--output` | `reddit_big_data.json` | Đường dẫn file JSON đầu ra |
| `--jsonl` | `reddit_big_data.jsonl` | Đường dẫn file JSONL đầu ra |
| `--client-id` | *(biến môi trường)* | Client ID OAuth của Reddit |
| `--client-secret` | *(biến môi trường)* | Client Secret OAuth của Reddit |
| `--reddit-user` | *(biến môi trường)* | Tên người dùng Reddit cho OAuth |
| `--reddit-pass` | *(biến môi trường)* | Mật khẩu Reddit cho OAuth |

**Thứ tự ưu tiên nguồn dữ liệu:** `--topic` > `--subreddits` > `--subreddit`

---

## Giải Thích Các File Đầu Ra

Chạy script sẽ tạo ra **ba file đầu ra**:

### `reddit_big_data.json`

Một **mảng JSON** chứa các bài đăng đã thu thập.

```json
[
  {
    "post_id": "abc123",
    "title": "Tin tức: ...",
    ...
    "comments": [ ... ],
    "author_profile": { ... }
  },
  ...
]
```

### `reddit_big_data.jsonl`

File **JSON phân dòng** (JSONL) — mỗi bài một dòng.

```jsonl
{"post_id": "abc123", "title": "...", ...}
{"post_id": "def456", "title": "...", ...}
```

### `crawler.log`

File log có cấu trúc ghi lại toàn bộ phiên thu thập. Hữu ích để gỡ lỗi, theo dõi tiến độ và kiểm tra dữ liệu đã được thu thập.

---

### Cấu Trúc Dữ Liệu Đầu Ra

Mỗi đối tượng bài đăng trong file đầu ra có cấu trúc sau:

#### Các Trường Cấp Bài Đăng

| Trường | Kiểu | Mô tả |
|---|---|---|
| `post_id` | `string` | ID ngắn của bài đăng trên Reddit (ví dụ: `"abc123"`) |
| `fullname` | `string` | Tên đầy đủ trên Reddit (ví dụ: `"t3_abc123"`) |
| `subreddit` | `string` | Tên subreddit (ví dụ: `"worldnews"`) |
| `subreddit_id` | `string` | ID nội bộ của subreddit |
| `subreddit_type` | `string` | `"public"`, `"restricted"` hoặc `"private"` |
| `subreddit_subscribers` | `int` | Số lượng thành viên của subreddit |
| `title` | `string` | Tiêu đề bài đăng |
| `selftext` | `string` | Nội dung văn bản bài đăng (với bài text; trống với bài link) |
| `url` | `string` | URL mà bài đăng liên kết tới |
| `permalink` | `string` | Đường dẫn đầy đủ trên Reddit tới bài đăng |
| `domain` | `string` | Tên miền của URL được liên kết (ví dụ: `"bbc.com"`) |
| `post_type` | `string` | `"self"` (bài văn bản) hoặc `"link"` |
| `thumbnail` | `string` | URL ảnh thu nhỏ |
| `flair_text` | `string` | Nhãn flair của bài đăng |
| `is_video` | `bool` | Bài đăng có chứa video không |
| `is_gallery` | `bool` | Bài đăng có phải album ảnh không |
| `spoiler` | `bool` | Đã đánh dấu là spoiler |
| `nsfw` | `bool` | Đánh dấu không phù hợp cho trẻ em (18+) |
| `stickied` | `bool` | Được ghim bởi moderator |
| `locked` | `bool` | Bình luận bị khóa |
| `archived` | `bool` | Bài đăng đã lưu trữ (cũ) |
| `score` | `int` | Điểm số ròng (upvote − downvote) |
| `upvotes` | `int` | Số lượng upvote thô |
| `downvotes` | `int` | Số lượng downvote thô |
| `upvote_ratio` | `float` | Ví dụ: `0.95` nghĩa là 95% upvote |
| `comment_count` | `int` | Tổng số bình luận do Reddit khai báo |
| `crossposts_count` | `int` | Số lần bài đăng được crosspost |
| `gilded` | `int` | Số giải thưởng Reddit Gold |
| `total_awards_received` | `int` | Tổng số giải thưởng (tất cả loại) |
| `all_awardings` | `list` | Chi tiết đầy đủ mọi giải thưởng |
| `author` | `string` | Tên người dùng của tác giả bài đăng |
| `author_fullname` | `string` | ID nội bộ của tác giả trên Reddit |
| `author_flair_text` | `string` | Flair của tác giả trong subreddit |
| `is_original_content` | `bool` | Tác giả gắn nhãn OC |
| `created_utc` | `string` | Thời gian tạo bài (ISO-8601 UTC) |
| `created_utc_raw` | `float` | Thời gian tạo bài (Unix timestamp) |
| `crawled_at` | `string` | Thời điểm thu thập bản ghi này (ISO-8601 UTC) |
| `comments` | `list` | Danh sách các đối tượng bình luận (xem bên dưới) |
| `comments_fetched` | `int` | Số bình luận thực tế đã lấy được |
| `author_profile` | `object` | Dữ liệu hồ sơ tác giả (xem bên dưới) |

#### Đối Tượng Bình Luận

| Trường | Kiểu | Mô tả |
|---|---|---|
| `comment_id` | `string` | ID ngắn của bình luận |
| `fullname` | `string` | Tên đầy đủ trên Reddit (ví dụ: `"t1_xyz"`) |
| `post_id` | `string` | ID của bài đăng cha |
| `parent_id` | `string` | Tên đầy đủ của cha (bài đăng hoặc bình luận) |
| `depth` | `int` | Cấp độ lồng nhau (0 = bình luận cấp cao nhất) |
| `author` | `string` | Tên người dùng của người bình luận |
| `body` | `string` | Nội dung văn bản của bình luận |
| `score` | `int` | Điểm số ròng của bình luận |
| `upvotes` | `int` | Số upvote thô |
| `gilded` | `int` | Giải thưởng Gold trên bình luận này |
| `total_awards` | `int` | Tổng giải thưởng |
| `stickied` | `bool` | Được ghim bởi moderator |
| `is_submitter` | `bool` | Người bình luận có phải tác giả bài đăng không |
| `controversiality` | `int` | `1` nếu gây tranh cãi, `0` nếu không |
| `created_utc` | `string` | Thời gian tạo bình luận (ISO-8601 UTC) |
| `replies` | `list` | Các đối tượng bình luận con lồng nhau (đệ quy) |
| `reply_count` | `int` | Số lượng phản hồi trực tiếp đã lấy được |

#### Đối Tượng Hồ Sơ Tác Giả

| Trường | Kiểu | Mô tả |
|---|---|---|
| `username` | `string` | Tên người dùng Reddit |
| `user_id` | `string` | ID nội bộ người dùng trên Reddit |
| `created_utc` | `string` | Thời gian tạo tài khoản (ISO-8601 UTC) |
| `comment_karma` | `int` | Karma kiếm được từ bình luận |
| `link_karma` | `int` | Karma kiếm được từ bài đăng |
| `total_karma` | `int` | Tổng karma |
| `is_mod` | `bool` | Người dùng có phải moderator ở đâu đó không |
| `is_gold` | `bool` | Có Reddit Premium |
| `is_employee` | `bool` | Là nhân viên Reddit |
| `verified` | `bool` | Email đã xác minh |
| `icon_img` | `string` | URL ảnh đại diện của người dùng |

---

## Hiểu Kết Quả Hiển Thị Trên Màn Hình

Trong quá trình thu thập bạn sẽ thấy các thông báo log có cấu trúc. Dưới đây là ý nghĩa của từng thông báo:

```
2024-01-15 10:23:01 [INFO] 🚀 Starting crawl | target=2000 posts | subreddits=['worldnews'] | sort=new | min_comments=15
```
Quá trình thu thập đang bắt đầu. Hiển thị mục tiêu, subreddits, thứ tự sắp xếp và bộ lọc bình luận tối thiểu.

```
2024-01-15 10:23:03 [INFO] 📄 Fetching listing | sub=r/worldnews | sort=new/all | collected=0/2000 | after=start
```
Đang lấy trang tiếp theo của danh sách bài từ Reddit. `after` là con trỏ phân trang.

```
2024-01-15 10:23:06 [INFO] ✅ 10/2000 posts collected | latest: Breaking: Some news headline...
```
Cứ mỗi 10 bài, một điểm kiểm tra tiến độ được in ra cùng tiêu đề bài mới nhất.

```
2024-01-15 10:23:10 [INFO] 💾 Saved 100 posts → reddit_big_data.json
```
Lưu điểm kiểm tra vào JSON mỗi 100 bài (phòng khi quá trình thu thập bị gián đoạn).

```
2024-01-15 10:23:11 [WARNING] ⚠️  No new posts on this page (stale=1/3)
```
Tất cả bài trên trang này đã được thấy rồi. Sau 3 trang "cũ" như vậy, chiến lược sắp xếp sẽ được xoay vòng.

```
2024-01-15 10:23:15 [WARNING] Rate limited on listing (attempt 1/4) — sleeping 30s…
```
Reddit trả về lỗi HTTP 429 (Quá nhiều yêu cầu). Script sẽ tự động chờ và thử lại.

```
2024-01-15 10:45:22 [WARNING] Rotating sort strategy for r/worldnews after 3 stale pages.
```
Trình thu thập đang chuyển từ (ví dụ) `new/all` sang `top/day` để tìm bài mới và vượt qua giới hạn ~1.000 bài của Reddit theo mỗi tổ hợp (subreddit, sort).

**Khi hoàn thành thu thập:**

```
============================================================
  🎉  CRAWL COMPLETE
============================================================
  Posts collected    : 2,000
  Comments fetched   : 87,432
  Unique authors     : 1,654
  Avg post score     : 4,218.3
  User profiles cached: 1,654
  Output (JSON)      : reddit_big_data.json
  Output (JSONL)     : reddit_big_data.jsonl
============================================================
```

| Trường tổng kết | Ý nghĩa |
|---|---|
| **Posts collected** | Tổng số bản ghi bài đăng đã lưu |
| **Comments fetched** | Tổng số bình luận trên tất cả các bài |
| **Unique authors** | Số tác giả bài đăng khác nhau |
| **Avg post score** | Điểm số ròng trung bình (upvote − downvote) của tất cả bài |
| **User profiles cached** | Số hồ sơ tác giả đã lấy (đã loại trùng lặp) |
| **Output (JSON)** | Đường dẫn tới file mảng JSON đầy đủ |
| **Output (JSONL)** | Đường dẫn tới file JSONL streaming |

---

## Giới Hạn Tốc Độ & Lưu Ý Khi Sử Dụng

| Chế độ | Độ trễ giữa các request | Thời gian ước tính cho 2.000 bài |
|---|---|---|
| **Ẩn danh** | 2,5 giây | ~3–6 giờ |
| **OAuth** | 0,6 giây | ~45–90 phút |

- Script xử lý **HTTP 429** (giới hạn tốc độ) tự động với cơ chế back-off lũy thừa (chờ tối đa 150 giây trước khi thử lại).
- Reddit giới hạn mỗi tổ hợp `(subreddit, sort, time_filter)` ở **~1.000 bài**. Script tự động xoay vòng qua 9 chiến lược sắp xếp (`new`, `top/day`, `top/week`, `top/month`, `top/year`, `top/all`, `hot`, `controversial/month`, `controversial/year`) để thu thập thêm bài độc đáo từ một subreddit.
- Dùng `--no-comments --no-users` nếu bạn chỉ cần siêu dữ liệu bài đăng và muốn thu thập nhanh.
- Đặt `--min-comments 0` nếu bạn muốn bài đăng ở mọi mức độ tương tác, kể cả bài không có bình luận.

---

## Xử Lý Sự Cố

| Vấn đề | Giải pháp |
|---|---|
| `ModuleNotFoundError: No module named 'requests'` | Chạy `pip install -r requirements.txt` |
| Thu thập chạy rất chậm | Dùng thông tin xác thực OAuth hoặc thêm `--no-comments --no-users` |
| `Rate limited on listing — sleeping 30s…` | Hành vi bình thường — script xử lý tự động |
| File đầu ra trống / quá nhỏ | Giảm `--min-comments` (mặc định là 15) hoặc thử subreddit có nhiều hoạt động hơn |
| `All subreddits exhausted — stopping early` | Reddit có thể có ít bài hơn mức yêu cầu; thêm subreddit bằng `--subreddits` |
| OAuth thất bại | Kiểm tra lại thông tin xác thực; đảm bảo bạn tạo app loại **"script"** trên Reddit |
| `JSONDecodeError` trong log | Thường là sự cố tạm thời của Reddit API; script sẽ tự động thử lại |
