# Movie Data Analysis

This is a JavaScript project that uses Prisma to interact with a database. The project analyzes movie data from CSV files.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Node.js
- pnpm

### Installing

1. Clone the repository:

```sh
git clone <repository-url>
```

2. Navigate to the project directory:

```sh
cd <project-directory>
```

3. Install the dependencies:

```sh
pnpm install
```

## Environment Variables

Create a `.env` file in the project directory and add the following environment variables:

```sh
DATABASE_URL=<database-url>
```

## Usage

The main script is `index.js`. It contains several functions for analyzing movie data, such as `topMoviesByDuration`, `topMoviesByYear`, and `topMoviesByRating`. These functions are currently commented out.

To use these functions, uncomment them in `getSolutions` and run the script:

```sh
pnpm run start
```

## Data

The movie data is stored in CSV files in the `data` directory:

- movies.csv
- ratings.csv

## Database Schema

The [PostgreSQL](https://www.postgresql.org/) database of this project is currently hosted on [render](https://render.com/) and its schema is defined in `prisma/schema.prisma`.

## Migrations

Database migrations are stored in the `prisma/migrations` directory.

## Built With

- [Prisma](https://www.prisma.io/) - Next-generation Node.js and TypeScript ORM for **PostgreSQL**, MySQL, MariaDB, SQL Server, and SQLite.
