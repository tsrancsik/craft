-- there are only documents created before signup
select
    user_id,
    signup_date,
    sum(documents_created) as document_created,
    sum(document_edits) as document_edits,
    sum(document_opens) as document_opens,
    sum(document_shares) as document_shares,
    sum(comments_created) as comments_created,
    sum(reactions_created) as reactions_created
from craft.sessions
where date < signup_date
group by 1, 2
having
    sum(document_edits) > 0
    or sum(document_opens) > 0
    or sum(document_shares) > 0
    or sum(comments_created) > 0
    or sum(reactions_created) > 0
order by 2
