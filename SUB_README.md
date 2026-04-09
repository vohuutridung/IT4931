# Hướng dẫn cào nền tảng Instagram bằng Apify.

---

## Đăng kí tài khoản apify

- B1: Đăng kí tài khoản apify ở đây: https://console.apify.com/
- B2: Vào link sau để lấy **API_TOKEN**: https://console.apify.com/settings/integrations

Note: Mỗi account sẽ được cấp 5$ credit khi tạo.

---

## Chạy notebook cào dữ liệu

- B1: Tải file crawl_ig.ipynb về, có thể chạy IDE local hoặc kaggle
- B2: Thay **API_TOKEN** ở trên vào biến APIFY_TOKEN
- B3: Copy danh sách hashtag của mình vào list hashtags ([Danh sách hashtags](https://docs.google.com/document/d/1NQNFZm3_r1a5sgFXxczXCf26_zf-ztq5qZRJiFWEynQ/edit?fbclid=IwY2xjawRDVkVleHRuA2FlbQIxMQBzcnRjBmFwcF9pZAEwAAEeYO-GHTgpQNruMABoysDT15JbmiO5C9Ixm-D4l7-EWhptzda7igtQBDde6SY_aem_7cn5eZhTZu_GD4Wia_jjVA&tab=t.y7haec3gpam8))
- B4: Chạy notebook để lấy kết quả là file tên ig_posts.jsonl

**Lưu ý**:
- Nên chạy notebook ở kaggle để không cần setup nhiều.
- Chú ý nếu chạy ở kaggle thì tải file output ig_posts.jsonl ở folder kaggle/working về trước khi tắt notebook, nếu không sẽ mất file output.
- Mỗi người sẽ tạo 3 apify account, và sẽ có 3 hashtag list tương ứng để chạy ở trong link [này](https://docs.google.com/document/d/1NQNFZm3_r1a5sgFXxczXCf26_zf-ztq5qZRJiFWEynQ/edit?fbclid=IwY2xjawRDVkVleHRuA2FlbQIxMQBzcnRjBmFwcF9pZAEwAAEeYO-GHTgpQNruMABoysDT15JbmiO5C9Ixm-D4l7-EWhptzda7igtQBDde6SY_aem_7cn5eZhTZu_GD4Wia_jjVA&tab=t.y7haec3gpam8).
