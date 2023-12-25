require("dotenv").config();
const { PrismaClient } = require("@prisma/client");
const papa = require("papaparse");
const fs = require("fs");
const streamToString = require("stream-to-string");
const { v4: uuidv4 } = require("uuid");

const prisma = new PrismaClient();

// <--- Functions to Import and manage Data --->
const deleteOldMovieRecords = async () => {
  try {
    await prisma.movie.deleteMany({});
    console.log("Old movie records deleted");
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

const createMovieRecords = async () => {
  try {
    const file = fs.createReadStream("./data/movies.csv");
    const fileString = await streamToString(file);

    const { data: movies } = papa.parse(fileString, {
      header: true,
    });

    const data = movies.map((movie) => {
      if (!movie?.id) {
        movie.id = uuidv4();
      }
      movie.year = parseInt(movie.year);
      movie.minutes = parseFloat(movie.minutes);
      return movie;
    });

    await prisma.movie.createMany({
      data,
      skipDuplicates: true,
    });

    console.log("Movie records created");
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

const getMovieRecordsCount = async () => {
  try {
    const moviesCount = (await prisma.movie.findMany()).length;
    console.log("moviesCount>>>", moviesCount);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

const deleteOldRatingsRecords = async () => {
  try {
    await prisma.rating.deleteMany({});
    console.log("Old rating records deleted");
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

const createRatingsRecords = async () => {
  try {
    const file = fs.createReadStream("./data/ratings.csv");
    const fileString = await streamToString(file);

    const { data: ratings } = papa.parse(fileString, {
      header: true,
    });

    const data = ratings.map((rating) => {
      rating.rating_id = uuidv4();
      if (!rating?.movie_id) {
        rating.movie_id = uuidv4();
      }
      rating.rating = parseFloat(rating.rating);
      rating.time = parseInt(rating.time);
      return rating;
    });

    console.log("data.length>>>", data.length);
    console.log("data[0]>>>", data[0]);
    const movie1 = await prisma.movie.findUnique({
      where: { id: data[0].movie_id },
    });
    console.log("movie1>>>", movie1);

    for (let i = 0; i < data.length; i++) {
      const rating = data[i];
      const movie = await prisma.movie.findUnique({
        where: { id: rating.movie_id },
      });
      if (movie) {
        console.log("index>>>", i);
        console.log("movie.id>>>", movie.id);
        await prisma.rating.create({
          data: {
            rating_id: rating.rating_id,
            rater_id: rating.rater_id,
            // movie_id: rating.movie_id,
            rating: rating.rating,
            time: rating.time,
            movie: {
              connect: {
                id: movie.id,
              },
            },
          },
        });
      }
    }

    console.log("Rating records created");
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
    process.exit(0);
  }
};

const getRatingsRecordsCount = async () => {
  try {
    const ratingsCount = (await prisma.rating.findMany()).length;
    console.log("ratingsCount>>>", ratingsCount);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};
// <--- Functions to Import and manage Data --->

// <--- Solutions --->
// a.1. top 5 movies titles by duration
const topMoviesByDuration = async () => {
  try {
    const result = await prisma.movie.findMany({
      take: 5,
      orderBy: {
        minutes: "desc",
      },
      where: {
        minutes: {
          not: null,
        },
      },
      select: {
        title: true,
      },
    });

    console.log("Top 5 Movie Titles by Duration:");
    result.forEach((movie, index) => {
      console.log(`${index + 1}. ${movie.title}`);
    });
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// a.2. top 5 movies titles by year
const topMoviesByYear = async () => {
  try {
    const result = await prisma.movie.findMany({
      take: 5,
      orderBy: {
        year: "desc",
      },
      where: {
        year: {
          not: null,
        },
      },
      select: {
        title: true,
      },
    });
    console.log("Top 5 Movie Titles by Year:");
    result.forEach((movie, index) => {
      console.log(`${index + 1}. ${movie.title}`);
    });
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// a.3. top 5 movies titles by rating
const topMoviesByRating = async () => {
  try {
    const result = await prisma.movie.findMany({
      where: {
        Rating: {
          some: {
            rating: {
              gte: 0,
            },
          },
        },
      },
      select: {
        id: true,
        title: true,
        Rating: {
          select: {
            rating: true,
          },
        },
      },
    });

    const filteredMovies = result.filter((movie) => movie.Rating.length >= 5);

    const moviesWithAverageRating = filteredMovies.map((movie) => ({
      id: movie.id,
      title: movie.title,
      averageRating:
        movie.Rating.reduce((avg, rating) => avg + rating.rating, 0) /
        movie.Rating.length,
    }));

    const sortedMovies = moviesWithAverageRating.sort(
      (a, b) => b.averageRating - a.averageRating
    );

    const topMovies = sortedMovies.slice(0, 5);

    console.log("Top 5 Movie Titles by Rating:");
    topMovies.forEach((movie, index) => {
      console.log(
        `${index + 1}. ${movie.title} - Average Rating: ${movie.averageRating}`
      );
    });
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// a.4. top 5 movies titles by number of ratings
const topMoviesByNumRatings = async () => {
  try {
    const result = await prisma.movie.findMany({
      select: {
        id: true,
        title: true,
        Rating: {
          select: {
            rating_id: true,
          },
        },
      },
    });

    const moviesWithNumberOfRatings = result.map((movie) => ({
      id: movie.id,
      title: movie.title,
      numberOfRatings: movie.Rating.length,
    }));

    const sortedMovies = moviesWithNumberOfRatings.sort(
      (a, b) => b.numberOfRatings - a.numberOfRatings
    );

    const topMovies = sortedMovies.slice(0, 5);

    console.log("Top 5 Movie Titles by Number of Ratings:");
    topMovies.forEach((movie, index) => {
      console.log(
        `${index + 1}. ${movie.title} - Number of Ratings: ${
          movie.numberOfRatings
        }`
      );
    });
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// b. number of unique raters
const getUniqueRaterCount = async () => {
  try {
    const result = await prisma.rating.findMany({
      distinct: ["rater_id"],
      select: {
        rater_id: true,
      },
    });

    console.log("Number of Unique Raters:", result.length);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// c.1. top 5 raters by number of ratings
const topRatersByCount = async () => {
  try {
    const result = await prisma.rating.groupBy({
      by: ["rater_id"],
      _count: {
        movie_id: true,
      },
      orderBy: {
        _count: {
          movie_id: "desc",
        },
      },
      take: 5,
      select: {
        rater_id: true,
        _count: true,
      },
    });

    console.log("Top 5 Rater IDs based on Most Movies Rated:");
    result.forEach((rater, index) => {
      console.log(
        `${index + 1}. Rater ID: ${rater.rater_id} - Movies Rated: ${
          rater._count.movie_id
        }`
      );
    });
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// c.2. top 5 raters by average rating
const topRatersByAvg = async () => {
  try {
    const allRaters = await prisma.rating.groupBy({
      by: ["rater_id"],
      _count: {
        movie_id: true,
      },
      select: {
        rater_id: true,
        _count: true,
      },
    });

    const eligibleRaters = allRaters.filter(
      (rater) => rater._count.movie_id >= 5
    );

    const topRaters = await prisma.rating.groupBy({
      by: ["rater_id"],
      _avg: {
        rating: true,
      },
      orderBy: {
        _avg: {
          rating: "desc",
        },
      },
      where: {
        rater_id: {
          in: eligibleRaters.map((rater) => rater.rater_id),
        },
      },
      take: 5,
      select: {
        rater_id: true,
        _avg: true,
      },
    });

    console.log(
      "Top 5 Rater IDs based on Highest Average Rating (min 5 ratings):"
    );
    topRaters.forEach((rater, index) => {
      console.log(
        `${index + 1}. Rater ID: ${rater.rater_id} - Average Rating: ${
          rater._avg.rating
        }`
      );
    });
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

const calAvgRating = (ratings) => {
  if (!ratings || ratings.length === 0) return 0;

  const totalRating = ratings.reduce((sum, rating) => sum + rating.rating, 0);
  return totalRating / ratings.length;
};
// d.1. top bay area movie
const topBayMovie = async (directorName = "Michael Bay") => {
  try {
    const moviesWithAvgRating = await prisma.movie.findMany({
      where: {
        director: directorName,
      },
      include: {
        Rating: {
          select: {
            rating: true,
          },
        },
      },
    });

    let topRatedMovie = moviesWithAvgRating[0];
    let topAverageRating = calAvgRating(topRatedMovie.Rating);

    for (const movie of moviesWithAvgRating) {
      const avgRating = calAvgRating(movie.Rating);

      if (avgRating > topAverageRating) {
        topRatedMovie = movie;
        topAverageRating = avgRating;
      }
    }

    console.log(`Top Rated Movie by ${directorName}: ${topRatedMovie.title}`);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// d.2. top comedy
const topComedy = async (genre = "Comedy") => {
  try {
    const moviesWithAvgRating = await prisma.movie.findMany({
      where: {
        genre: genre,
      },
      include: {
        Rating: {
          select: {
            rating: true,
          },
        },
      },
    });

    let topRatedMovie = moviesWithAvgRating[0];
    let topAverageRating = calAvgRating(topRatedMovie.Rating);

    for (const movie of moviesWithAvgRating) {
      const avgRating = calAvgRating(movie.Rating);

      if (avgRating > topAverageRating) {
        topRatedMovie = movie;
        topAverageRating = avgRating;
      }
    }

    console.log(`Top Rated ${genre} Movie: ${topRatedMovie.title}`);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// d.3. top movie in a specific year
const topMovieByYear = async (targetYear = 2013) => {
  try {
    const moviesWithAvgRating = await prisma.movie.findMany({
      where: {
        year: targetYear,
      },
      include: {
        Rating: {
          select: {
            rating: true,
          },
        },
      },
    });

    if (moviesWithAvgRating.length === 0) {
      console.log(`No movies found for the year ${targetYear}`);
      return;
    }

    let topRatedMovie = moviesWithAvgRating[0];
    let topAverageRating = calAvgRating(topRatedMovie.Rating);

    for (const movie of moviesWithAvgRating) {
      const avgRating = calAvgRating(movie.Rating);

      if (avgRating > topAverageRating) {
        topRatedMovie = movie;
        topAverageRating = avgRating;
      }
    }

    console.log(`Top Rated Movie in ${targetYear}: ${topRatedMovie.title}`);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// d.4. top movie in India with minimum 5 ratings
const topMovieInIndia = async (country = "India", minRatings = 5) => {
  try {
    const moviesWithAvgRating = await prisma.movie.findMany({
      where: {
        country: country,
      },
      include: {
        Rating: {
          select: {
            rating: true,
          },
        },
      },
    });

    if (moviesWithAvgRating.length === 0) {
      console.log(`No movies found for the specified country: ${country}`);
      return;
    }

    let topRatedMovie = null;
    let topAverageRating = 0;

    for (const movie of moviesWithAvgRating) {
      const avgRating = calAvgRating(movie.Rating);

      if (movie.Rating.length >= minRatings && avgRating > topAverageRating) {
        topRatedMovie = movie;
        topAverageRating = avgRating;
      }
    }

    if (topRatedMovie) {
      console.log(
        `Top Rated Movie in ${country} with at least ${minRatings} ratings: ${topRatedMovie.title}`
      );
    } else {
      console.log(
        `No movies in ${country} with at least ${minRatings} ratings`
      );
    }
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

// e. Favorite Movie Genre of Rater ID 1040
const favoriteGenreForRater = async (raterId = "1040") => {
  try {
    const ratingsByRater = await prisma.rating.findMany({
      where: {
        rater_id: raterId,
      },
      include: {
        movie: {
          select: {
            genre: true,
          },
        },
      },
    });

    if (ratingsByRater.length === 0) {
      console.log(`No ratings found for this rater: ${raterId}`);
      return;
    }

    const genreCounts = {};

    ratingsByRater.forEach((rating) => {
      const genre = rating.movie.genre;
      if (genre) {
        genreCounts[genre] = (genreCounts[genre] || 0) + 1;
      }
    });

    const favoriteGenre = Object.keys(genreCounts).reduce((a, b) =>
      genreCounts[a] > genreCounts[b] ? a : b
    );

    console.log(
      `Favorite Movie Genre for Rater ID ${raterId}: ${favoriteGenre}`
    );
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
    process.exit(0);
  }
};

// f. top genre by rating
const highestAvgRatingForGenreByRater = async (
  raterId = "1040",
  minRatings = 5
) => {
  try {
    const ratingsByRater = await prisma.rating.findMany({
      where: {
        rater_id: raterId,
      },
      include: {
        movie: {
          select: {
            genre: true,
          },
        },
      },
    });

    if (ratingsByRater.length === 0) {
      console.log(`No ratings found for the specified rater ID: ${raterId}`);
      return;
    }

    const genreAvgRatings = {};

    ratingsByRater.forEach((rating) => {
      const genre = rating.movie.genre;
      if (genre) {
        if (!genreAvgRatings[genre]) {
          genreAvgRatings[genre] = {
            totalRating: 0,
            count: 0,
          };
        }
        genreAvgRatings[genre].totalRating += rating.rating;
        genreAvgRatings[genre].count += 1;
      }
    });

    let highestAvgRating = 0;
    let highestAvgRatingGenre = null;

    Object.keys(genreAvgRatings).forEach((genre) => {
      const avgRating =
        genreAvgRatings[genre].totalRating / genreAvgRatings[genre].count;

      if (
        genreAvgRatings[genre].count >= minRatings &&
        avgRating > highestAvgRating
      ) {
        highestAvgRating = avgRating;
        highestAvgRatingGenre = genre;
      }
    });

    if (highestAvgRatingGenre) {
      console.log(
        `Highest Average Rating for a Movie Genre by Rater ID ${raterId} is ${highestAvgRatingGenre} with ${highestAvgRating.toFixed(
          2
        )} average rating`
      );
    } else {
      console.log(
        `No genres with at least ${minRatings} ratings for Rater ID ${raterId}`
      );
    }
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
    process.exit(0);
  }
};

// g. second highest year of action movies
const yearWithSecondHighestActionMovies = async () => {
  try {
    const actionMovies = await prisma.movie.findMany({
      where: {
        genre: "Action",
        country: "USA",
        minutes: {
          lt: 120,
        },
        Rating: {
          some: {
            rating: {
              gte: 6.5,
            },
          },
        },
      },
      select: {
        year: true,
      },
    });

    if (actionMovies.length === 0) {
      console.log(
        "No movie found where genre is Action, country is USA, duration is less than 120 minutes and rating is greater than or equal to 6.5"
      );
      return;
    }

    const yearCounts = actionMovies.reduce((counts, movie) => {
      counts[movie.year] = (counts[movie.year] || 0) + 1;
      return counts;
    }, {});

    const sortedYears = Object.keys(yearCounts).sort(
      (a, b) => yearCounts[b] - yearCounts[a]
    );

    // Find the second-highest year
    const secondHighestYear = sortedYears[1];

    console.log("Second-Highest Year with Action Movies:", secondHighestYear);
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
    process.exit(0);
  }
};

// h. highly rated movies
const highlyRatedMovies = async () => {
  try {
    const highRatedMovies = await prisma.rating.findMany({
      where: {
        rating: {
          gte: 7,
        },
      },
      select: {
        movie_id: true,
      },
    });

    const movieIds = highRatedMovies.map((rating) => rating.movie_id);

    const highRatedMoviesCount = await prisma.movie.count({
      where: {
        id: {
          in: movieIds,
        },
        Rating: {
          some: {
            rating: {
              gte: 5,
            },
          },
        },
      },
    });
    console.log("Number of Movies with High Ratings:", highRatedMoviesCount);
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await prisma.$disconnect();
  }
};
// <--- Solutions --->

const getSolutions = async () => {
  try {
    // await topMoviesByDuration();
    // await topMoviesByYear();
    // await topMoviesByRating();
    // await topMoviesByNumRatings();
    // await getUniqueRaterCount();
    // await topRatersByCount();
    // await topRatersByAvg();
    // await topBayMovie();
    // await topComedy();
    // await topMovieByYear();
    // await topMovieInIndia();
    // await favoriteGenreForRater();
    // await highestAvgRatingForGenreByRater();
    // await yearWithSecondHighestActionMovies();
    // await highlyRatedMovies();
  } catch (error) {
    console.log("error>>>>", error);
    process.exit(1);
  }
};

getSolutions();

// <--- DANGER ZONE --->

// DO NOT RUN THESE. It will remove the old data from the DB and insert new data again which will take time.
// (async () => {
// try {
//   await deleteOldMovieRecords();
//   await createMovieRecords();
//   await getMovieRecordsCount();
// } catch (error) {
//   console.log("error>>>>", error);
//   process.exit(1);
// }
// })();

// (async () => {
// try {
//   await deleteOldRatingsRecords();
//   await createRatingsRecords();
//   await getRatingsRecordsCount();
// } catch (error) {
//   console.log("error>>>>", error);
//   process.exit(1);
// })();
