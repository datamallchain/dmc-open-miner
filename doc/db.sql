-- # miner

-- ## node

CREATE TABLE IF NOT EXISTS `sector_node_${node_id}` (
    `sector_id` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    `local_path` VARCHAR(512) NOT NULL UNIQUE,  
    `capacity` BIGINT UNSIGNED NOT NULL, 
    `chunk_size` INT UNSIGNED NOT NULL,
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ## gateway

CREATE TABLE IF NOT EXISTS `nodes_gateway` (
    `node_id` INT UNSIGNED NOT NULL UNIQUE,
    `host` VARCHAR(512) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- cache for sector_node_${node_id}
CREATE TABLE IF NOT EXISTS `sectors_gateway` (
    `gateway_sector_id` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    `node_id` INT UNSIGNED NOT NULL,
    `node_sector_id` INT UNSIGNED, -- foreign sector_node_${node_id}.sector_id
    `local_path` VARCHAR(512) NOT NULL,  
    `capacity` BIGINT UNSIGNED NOT NULL, 
    `chunk_size` INT UNSIGNED NOT NULL,
    UNIQUE INDEX `location_index` (`node_id`, `local_path`),
    FOREIGN KEY (`node_id`) REFERENCES nodes_gateway(node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ## account

CREATE TABLE IF NOT EXISTS `active_sector` (
    `sector_id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    `raw_sector_id` BIGINT UNSIGNED NOT NULL,
    `bill_options` BLOB, 
    `bill_id` BIGINT UNSIGNED, 
    `block_number` BIGINT UNSIGNED, 
    `tx_index` INT UNSIGNED, 
    `state_code` TINYINT UNSIGNED NOT NULL, 
    `state_value` BLOB,      
    `process_id` INT UNSIGNED, 
    `update_at` BIGINT UNSIGNED NOT NULL, 
    `removed_sector_id` BIGINT UNSIGNED, -- set to sector_id when removed
    INDEX (`bill_id`),
    INDEX(`update_at`),
    UNIQUE INDEX `unique_active_raw` (`raw_sector_id`, `removed_sector_id`),
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ## contract

CREATE TABLE IF NOT EXISTS `contract_state` (
    `order_id` BIGINT UNSIGNED NOT NULL PRIMARY KEY, 
    `state_code` TINYINT UNSIGNED NOT NULL,  
    `state_value` BLOB, 
    `process_id` INT UNSIGNED DEFAULT NULL, 
    `writen` BIGINT UNSIGNED DEFAULT 0, 
    `update_at` BIGINT UNSIGNED NOT NULL, 
    `block_number` BIGINT UNSIGNED NOT NULL, 
    `tx_index` INT UNSIGNED NOT NULL, 
    `bill_id` BIGINT UNSIGNED NOT NULL, 
    INDEX (`update_at`), 
    INDEX (`process_id`),
    UNIQUE INDEX `chain_index`(`block_number`, `tx_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `contract_sector` (
    `sector_id` BIGINT UNSIGNED NOT NULL, -- foreign sectors_gateway.node_id + sectors_gateway.sector_id
    `sector_offset_start` BIGINT UNSIGNED NOT NULL,
    `sector_offset_end` BIGINT UNSIGNED NOT NULL, 
    `order_id` BIGINT UNSIGNED NOT NULL UNIQUE,  
    `chunk_size` INT UNSIGNED NOT NULL, 
    PRIMARY KEY (`sector_id`, `sector_offset_start`, `sector_offset_end`), 
    INDEX (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `contract_stub` (
    `order_id` BIGINT UNSIGNED NOT NULL, 
    `index` INT UNSIGNED NOT NULL, 
    `content` BLOB, 
    INDEX (`order_id`),
    UNIQUE INDEX `source_index` (`order_id`, `index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `contract_source_challenge` (
    `challenge_id` BIGINT UNSIGNED NOT NULL PRIMARY KEY, 
    `order_id` BIGINT UNSIGNED NOT NULL,
    `challenge_params` BLOB NOT NULL,
    `state_code` TINYINT UNSIGNED NOT NULL,
    `state_value` BLOB,
    `start_at` BIGINT UNSIGNED NOT NULL,
    `update_at` BIGINT UNSIGNED NOT NULL,
    `process_id` INT UNSIGNED DEFAULT NULL,
    `block_number` BIGINT UNSIGNED DEFAULT 0, 
    `tx_index` INT UNSIGNED DEFAULT 0, 
    INDEX (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;