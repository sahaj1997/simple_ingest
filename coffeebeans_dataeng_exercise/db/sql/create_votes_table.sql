-- Create the votes table if it does not already exist in the specified schema
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    Id STRING PRIMARY KEY,  -- Unique identifier for each record, set as the primary key
    UserId STRING NULL,  -- Identifier for the user who cast the vote, can be NULL
    PostId STRING NULL,  -- Identifier for the post that received the vote, can be NULL
    VoteTypeId STRING NULL,  -- Identifier for the type of vote (e.g., upvote, downvote), can be NULL
    BountyAmount STRING NULL,  -- Amount of bounty associated with the vote, can be NULL
    CreationDate TIMESTAMP NULL  -- Timestamp indicating when the vote was created, can be NULL
);
