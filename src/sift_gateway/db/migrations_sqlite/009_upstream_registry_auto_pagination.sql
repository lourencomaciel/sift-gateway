-- 009_upstream_registry_auto_pagination.sql: add auto-pagination fields.

ALTER TABLE upstream_registry
    ADD COLUMN auto_paginate_max_pages INTEGER NULL CHECK (
        auto_paginate_max_pages IS NULL OR auto_paginate_max_pages >= 0
    );

ALTER TABLE upstream_registry
    ADD COLUMN auto_paginate_max_records INTEGER NULL CHECK (
        auto_paginate_max_records IS NULL OR auto_paginate_max_records >= 0
    );

ALTER TABLE upstream_registry
    ADD COLUMN auto_paginate_timeout_seconds REAL NULL CHECK (
        auto_paginate_timeout_seconds IS NULL
        OR auto_paginate_timeout_seconds > 0
    );
