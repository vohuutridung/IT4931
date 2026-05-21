from locust import HttpUser, between, task


class ApiUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def realtime_stats(self):
        self.client.get("/api/v1/stats/realtime")

    @task
    def top_hashtags(self):
        self.client.get("/api/v1/hashtags/top?window_hours=24&top_n=20")
