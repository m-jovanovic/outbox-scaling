CREATE TABLE IF NOT EXISTS outbox_messages (
    id UUID PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    content JSONB NOT NULL,
    occurred_on_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_on_utc TIMESTAMP WITH TIME ZONE NULL,
    error TEXT NULL
);

-- Create a filtered index on unprocessed messages, including all necessary columns
CREATE INDEX IF NOT EXISTS idx_outbox_messages_unprocessed 
ON public.outbox_messages (occurred_on_utc, processed_on_utc)
INCLUDE (id, type, content)
WHERE processed_on_utc IS NULL;