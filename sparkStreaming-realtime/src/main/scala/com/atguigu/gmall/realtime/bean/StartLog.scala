package com.atguigu.gmall.realtime.bean

case class StartLog(
                     mid: String,
                     user_id: String,
                     province_id: String,
                     channel: String,
                     is_new: String,
                     model: String,
                     operate_system: String,
                     version_code: String,
                     brand: String,
                     entry: String,
                     loading_time: Long,
                     open_ad_id: String,
                     open_ad_ms: Long,
                     open_ad_skip_ms: Long,
                     ts: Long
                   )