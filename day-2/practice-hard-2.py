import datetime
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def get_utc_date_str():
    """Helper to consistently get current UTC date as YYYY-MM-DD"""
    return datetime.datetime.now(datetime.timezone.utc).date().isoformat()

def record_view(prompt_id: str):
    today = get_utc_date_str()
    daily_key = f"trending:{today}"
    prompt_key = f"prompt:{prompt_id}"
    
    pipe = r.pipeline()
    pipe.hincrby(prompt_key, "uses", 1)
    pipe.zincrby(daily_key, 1, prompt_id)
    pipe.expire(daily_key, 604800, nx=True)
    pipe.execute()

def _fetch_prompts_with_titles(scored_ids):
    """Helper to fetch titles for a list of (prompt_id, score) tuples"""
    if not scored_ids:
        return []
    pipe = r.pipeline()
    for pid, _ in scored_ids:
        pipe.hget(f"prompt:{pid}", "title")
    titles = pipe.execute()
    return [
        {
            "id": pid,
            "title": title or "Unknown Title", 
            "score": int(score)
        }
        for (pid, score), title in zip(scored_ids, titles)
    ]

def trending_today(n: int):
    today = get_utc_date_str()
    daily_key = f"trending:{today}"
    scored_ids = r.zrevrange(daily_key, 0, n - 1, withscores=True)
    return _fetch_prompts_with_titles(scored_ids)

def trending_week(n: int):
    temp_key = "trending:week_cache"
    if not r.exists(temp_key):
        today = datetime.datetime.now(datetime.timezone.utc).date()
        keys = [
            f"trending:{(today - datetime.timedelta(days=i)).isoformat()}" 
            for i in range(7)
        ]
        
        pipe = r.pipeline()
        pipe.zunionstore(temp_key, keys)
        pipe.expire(temp_key, 60)
        pipe.execute()
        
    scored_ids = r.zrevrange(temp_key, 0, n - 1, withscores=True)
    
    return _fetch_prompts_with_titles(scored_ids)