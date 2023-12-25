-- CreateTable
CREATE TABLE "Movie" (
    "id" TEXT NOT NULL,
    "title" TEXT,
    "year" INTEGER,
    "country" TEXT,
    "genre" TEXT,
    "director" TEXT,
    "minutes" DOUBLE PRECISION,
    "poster" TEXT,

    CONSTRAINT "Movie_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Rating" (
    "rating_id" TEXT NOT NULL,
    "rater_id" TEXT,
    "movie_id" TEXT,
    "rating" DOUBLE PRECISION,
    "time" INTEGER,

    CONSTRAINT "Rating_pkey" PRIMARY KEY ("rating_id")
);

-- AddForeignKey
ALTER TABLE "Rating" ADD CONSTRAINT "Rating_movie_id_fkey" FOREIGN KEY ("movie_id") REFERENCES "Movie"("id") ON DELETE SET NULL ON UPDATE CASCADE;
