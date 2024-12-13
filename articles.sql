-- update_article function for updated_at column trigger
CREATE OR REPLACE FUNCTION update_article()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = now();
   RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- articles trigger
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON articles
FOR EACH ROW
EXECUTE FUNCTION update_article();

-- articles table
CREATE TABLE articles (
    id SERIAL PRIMARY KEY, -- unique id for each article
    title VARCHAR NOT NULL, -- title of the article
    content TEXT NOT NULL, -- content of the article
    published_at TIMESTAMP, -- publication date of article
    author_id INT NOT NULL, -- id of article author
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, -- timestamp when the article was created
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, -- timestamp when the article was updated
    deleted_at TIMESTAMP -- timestamp when the article was deleted
);

INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at) VALUES
	('Artikel Pertama', 'ini adalah konten artikel pertama.', '2020-04-13 10:30:00', 1, DEFAULT, DEFAULT),
	('Artikel Kedua', 'ini adalah konten artikel kedua.', '2022-02-05 09:00:00', 2, DEFAULT, DEFAULT),
	('Artikel Ketiga', 'ini adalah konten artikel ketiga.', '2023-07-08 22:15:00', 1, DEFAULT, DEFAULT),
	('Artikel Keempat', 'ini adalah konten artikel keempat.', '2024-04-22 13:00:00', 3, DEFAULT, DEFAULT);

SELECT * FROM articles; 
