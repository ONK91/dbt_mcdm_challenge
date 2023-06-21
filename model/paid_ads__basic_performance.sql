WITH bing AS (
    SELECT  date AS date
            ,NULL AS add_to_cart
            ,clicks AS clicks
            ,NULL AS comments
            ,NULL AS engagements
            ,NULL AS impressions
            ,NULL AS installs
            ,NULL AS likes
            ,NULL AS link_clicks
            ,NULL AS post_click_conversions
            ,NULL AS post_view_conversions
            ,NULL AS posts
            ,NULL AS purchase
            ,NULL AS registrations
            ,revenue AS revenue
            ,NULL AS shares
            ,spend AS spend
            ,conv AS total_conversions
            ,NULL AS video_views
            ,ad_id AS ad_id
            ,adset_id AS adset_id
            ,campaign_id AS campaign_id
            ,channel AS channel
            ,NULL AS creative_id
            ,NULL AS placement_id
    FROM    {{ref('src_ads_bing_all_data')}}
)
,tiktok AS (
    SELECT  date AS date
            ,add_to_cart AS add_to_cart
            ,clicks AS clicks
            ,NULL AS comments
            ,NULL AS engagements
            ,impressions AS impressions
            ,NULL AS installs
            ,NULL AS likes
            ,NULL AS link_clicks
            ,NULL AS post_click_conversions
            ,NULL AS post_view_conversions
            ,NULL AS posts
            ,purchase AS purchase
            ,registrations AS registrations
            ,NULL AS revenue
            ,NULL AS shares
            ,spend AS spend
            ,conversions AS total_conversions
            ,video_views AS video_views
            ,ad_id AS ad_id
            ,NULL AS adset_id
            ,campaign_id AS campaign_id
            ,channel AS channel
            ,NULL AS creative_id
            ,NULL AS placement_id
    FROM    {{ref('src_ads_tiktok_ads_all_data')}}
)
,facebook AS (
    SELECT  date AS date
            ,add_to_cart AS add_to_cart
            ,inline_link_clicks AS clicks
            ,comments AS comments
            ,NULL AS engagements
            ,impressions AS impressions
            ,NULL AS installs
            ,likes AS likes
            ,NULL AS link_clicks
            ,NULL AS post_click_conversions
            ,NULL AS post_view_conversions
            ,NULL AS posts
            ,purchase AS purchase
            ,NULL AS registrations
            ,NULL AS revenue
            ,shares AS shares
            ,spend AS spend
            ,NULL AS total_conversions
            ,views AS video_views
            ,ad_id AS ad_id
            ,adset_id AS adset_id
            ,campaign_id AS campaign_id
            ,channel AS channel
            ,creative_id AS creative_id
            ,NULL AS placement_id
    FROM    {{ref('src_ads_creative_facebook_all_data')}}
)
,twitter AS (
    SELECT  date AS date
            ,NULL AS add_to_cart
            ,clicks AS clicks
            ,comments AS comments
            ,engagements AS engagements
            ,impressions AS impressions
            ,NULL AS installs
            ,likes AS likes
            ,NULL AS link_clicks
            ,NULL AS post_click_conversions
            ,NULL AS post_view_conversions
            ,NULL AS posts
            ,NULL AS purchase
            ,NULL AS registrations
            ,NULL AS revenue
            ,NULL AS shares
            ,spend AS spend
            ,NULL AS total_conversions
            ,video_total_views AS video_views
            ,NULL AS ad_id
            ,NULL AS adset_id
            ,campaign_id AS campaign_id
            ,channel AS channel
            ,NULL AS creative_id
            ,NULL AS placement_id
    FROM    {{ref('src_promoted_tweets_twitter_all_data')}}
)
SELECT * FROM bing
UNION ALL
SELECT * FROM tiktok
UNION ALL
SELECT * FROM facebook
UNION ALL
SELECT * FROM twitter