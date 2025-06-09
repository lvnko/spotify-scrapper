SELECT
    u.id, u.name,
    pl.id AS playlist_id,
    pl.name, sgcnt.count
FROM user u
LEFT JOIN playlist pl
    ON pl.user_id = u.id
LEFT JOIN (
    SELECT
        playlist_id, count(id) AS count
    FROM playlist_entry
    GROUP BY playlist_id
) sgcnt
    ON sgcnt.playlist_id = pl.id;