-- Insert data into the specified votes table from a JSON file with deduplication and upsert logic
INSERT INTO {schema}.{table} (
  -- Select the columns to insert into the table
  SELECT 
    Id,  -- Unique identifier for the record
    UserId,  -- Identifier for the user who cast the vote
    PostId,  -- Identifier for the post that received the vote
    VoteTypeId,  -- Identifier for the type of vote (e.g., upvote, downvote)
    BountyAmount,  -- Amount of bounty associated with the vote
    CreationDate  -- Timestamp when the vote was created
  FROM 
    (
      -- Select columns and assign a row number to each record, partitioned by Id and ordered by CreationDate descending
      SELECT 
        Id,  -- Unique identifier for the record
        UserId,  -- Identifier for the user who cast the vote
        PostId,  -- Identifier for the post that received the vote
        VoteTypeId,  -- Identifier for the type of vote
        BountyAmount,  -- Amount of bounty associated with the vote
        CreationDate,  -- Timestamp when the vote was created
        ROW_NUMBER() OVER (
          PARTITION BY Id  -- Partition the data by Id to ensure uniqueness
          ORDER BY 
            CreationDate DESC  -- Order by CreationDate in descending order to get the latest record
        ) as rn  -- Assign a row number to each record within the partition
      FROM 
        (
          -- Read data from a JSON file and select the required columns
          SELECT 
            Id,  -- Unique identifier for the record
            UserId,  -- Identifier for the user who cast the vote
            PostId,  -- Identifier for the post that received the vote
            VoteTypeId,  -- Identifier for the type of vote
            BountyAmount,  -- Amount of bounty associated with the vote
            CreationDate  -- Timestamp when the vote was created
          FROM 
            read_json('{file_path}')  -- Read data from the specified JSON file
        )
    ) 
  WHERE 
    rn = 1  -- Filter to keep only the latest record (rn = 1)
) 
ON CONFLICT (Id) DO 
  UPDATE 
  SET 
    UserId = EXCLUDED.UserId,  -- Update the UserId with the value from the excluded (new) row
    PostId = EXCLUDED.PostId,  -- Update the PostId with the value from the excluded row
    VoteTypeId = EXCLUDED.VoteTypeId,  -- Update the VoteTypeId with the value from the excluded row
    BountyAmount = EXCLUDED.BountyAmount,  -- Update the BountyAmount with the value from the excluded row
    CreationDate = EXCLUDED.CreationDate;  -- Update the CreationDate with the value from the excluded row
