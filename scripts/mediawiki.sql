--
-- Core of the wiki: each page has an entry here which identifies
-- it by title and contains some essential metadata.
--
CREATE TABLE /*$wgDBprefix*/page (
  -- Unique identifier number. The page_id will be preserved across
  -- edits and rename operations, but not deletions and recreations.
  page_id int(8) unsigned NOT NULL,
  
  -- A page name is broken into a namespace and a title.
  -- The namespace keys are UI-language-independent constants,
  -- defined in includes/Defines.php
  page_namespace int NOT NULL,
  
  -- The rest of the title, as text.
  -- Spaces are transformed into underscores in title storage.
  page_title varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  
  -- Comma-separated set of permission keys indicating who
  -- can move or edit the page.
  page_restrictions tinyblob NOT NULL default '',
  
  -- Number of times this page has been viewed.
  page_counter bigint(20) unsigned NOT NULL default '0',
  
  -- 1 indicates the article is a redirect.
  page_is_redirect tinyint(1) unsigned NOT NULL default '0',
  
  -- 1 indicates this is a new entry, with only one edit.
  -- Not all pages with one edit are new pages.
  page_is_new tinyint(1) unsigned NOT NULL default '0',
  
  -- Random value between 0 and 1, used for Special:Randompage
  page_random real unsigned NOT NULL,
  
  -- This timestamp is updated whenever the page changes in
  -- a way requiring it to be re-rendered, invalidating caches.
  -- Aside from editing this includes permission changes,
  -- creation or deletion of linked pages, and alteration
  -- of contained templates.
  page_touched char(14) binary NOT NULL default '',

  -- Handy key to revision.rev_id of the current revision.
  -- This may be 0 during page creation, but that shouldn't
  -- happen outside of a transaction... hopefully.
  page_latest int(8) unsigned NOT NULL,
  
  -- Uncompressed length in bytes of the page's current source text.
  page_len int(8) unsigned NOT NULL

);

--
-- Every edit of a page creates also a revision row.
-- This stores metadata about the revision, and a reference
-- to the text storage backend.
--
CREATE TABLE /*$wgDBprefix*/revision (
  rev_id int(8) unsigned NOT NULL,
  
  -- Key to page_id. This should _never_ be invalid.
  rev_page int(8) unsigned NOT NULL,
  
  -- Key to text.old_id, where the actual bulk text is stored.
  -- It's possible for multiple revisions to use the same text,
  -- for instance revisions where only metadata is altered
  -- or a rollback to a previous version.
  rev_text_id int(8) unsigned NOT NULL,
  
  -- Text comment summarizing the change.
  -- This text is shown in the history and other changes lists,
  -- rendered in a subset of wiki markup by Linker::formatComment()
  rev_comment tinyblob NOT NULL default '',
  
  -- Key to user.user_id of the user who made this edit.
  -- Stores 0 for anonymous edits and for some mass imports.
  rev_user int(5) unsigned NOT NULL default '0',
  
  -- Text username or IP address of the editor.
  rev_user_text varchar(255) binary NOT NULL default '',
  
  -- Timestamp
  rev_timestamp char(19) binary NOT NULL default '',
  
  -- Records whether the user marked the 'minor edit' checkbox.
  -- Many automated edits are marked as minor.
  rev_minor_edit tinyint(1) unsigned NOT NULL default '0'
  
);


--
-- Holds text of individual page revisions.
--
-- Field names are a holdover from the 'old' revisions table in
-- MediaWiki 1.4 and earlier: an upgrade will transform that
-- table into the 'text' table to minimize unnecessary churning
-- and downtime. If upgrading, the other fields will be left unused.
--
CREATE TABLE /*$wgDBprefix*/text (
  -- Unique text storage key number.
  -- Note that the 'oldid' parameter used in URLs does *not*
  -- refer to this number anymore, but to rev_id.
  --
  -- revision.rev_text_id is a key to this column
  old_id int(8) unsigned NOT NULL ,
  
  -- Depending on the contents of the old_flags field, the text
  -- may be convenient plain text, or it may be funkily encoded.
  old_text mediumblob NOT NULL default '',
  
  -- Comma-separated list of flags:
  -- gzip: text is compressed with PHP's gzdeflate() function.
  -- utf8: text was stored as UTF-8.
  --       If $wgLegacyEncoding option is on, rows *without* this flag
  --       will be converted to UTF-8 transparently at load time.
  -- object: text field contained a serialized PHP object.
  --         The object either contains multiple versions compressed
  --         together to achieve a better compression ratio, or it refers
  --         to another row where the text can be found.
  old_flags tinyblob NOT NULL default ''
  

);


CREATE TABLE `langlinks` (
  `ll_from` int(10) unsigned NOT NULL DEFAULT '0',
  `ll_lang` varbinary(20) NOT NULL DEFAULT '',
  `ll_title` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  KEY `ll_lang` (`ll_title`),
  KEY `ll_from2` (`ll_from`),
  KEY `ll_lang2` (`ll_lang`)
);
