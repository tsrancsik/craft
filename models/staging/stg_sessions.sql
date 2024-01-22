select
    user_id,
    signup_date :: date as signup_date,
    "date" :: date as session_date,
    "date" :: date - signup_date :: date as days_from_signup,
    document_edits,
    document_opens,
    comments_created,
    reactions_created
from
    {{ source('craft', 'sessions') }}