module.exports = [
`-- Up
  CREATE TABLE \`Setting\` (
    \`key\` TEXT NOT NULL UNIQUE,
    \`value\` BLOB,
    \`type\` INT NOT NULL DEFAULT 0,
    PRIMARY KEY(\`key\`)
  );
  CREATE INDEX IF NOT EXISTS Setting_index_key ON \`Setting\` (\`key\`);
  
-- Down
  DROP INDEX IF EXISTS Setting_index_key;
  DROP TABLE IF EXISTS Setting;
`,
`-- Up
INSERT INTO \`Setting\` (\`key\`, \`value\`, \`type\`) VALUES ('test', 'now', 0)

-- Down
DELETE FROM \`Setting\` WHERE \`key\` = 'test'
`
]
