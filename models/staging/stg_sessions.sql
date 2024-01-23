select
    user_id,
    signup_date :: date as signup_date,
    "date" :: date as session_date,
    "date" :: date - signup_date :: date as days_from_signup,
    -- inconsistent naming conventions
    documents_created,
    document_edits,
    document_shares,
    document_opens,
    comments_created,
    reactions_created
from
    {{ source('craft', 'sessions') }}