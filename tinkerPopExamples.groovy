
// generate random subgraph with coin step. No comments for now. Could return later
g.V().has('genre', 'name', 'Sci-Fi').
        in('genre').coin(0.02).
        in('rated').coin(0.001).
        out('occupation').
        path().unfold()

/**
 * OLTP Vertex Centric Queries
*/

// What “Star Trek” movies did user rate?

//Start with one user vertex with id 710. We have no names in the dataset
g.V().has('user', 'id', 710).
// traverse to all vertices by 'rated' edge.
        out('rated').
// Filter only "Star Trek" movies
        has('name', regex('Star Trek.*'))

// let's what was visited byt reaversal
g.V().has('user', 'id', 710).
        out('rated').
        has('name', regex('Star Trek.*')).
// path() step give us all vistited pathes,
//unfold() "flatMap" them into set of verticies
        path().unfold()

// add gendre and see as one more set of hops were added
g.V().has('user', 'id', 710).
        out('rated').
        has('name', regex('Star Trek.*')).
        out("genre").
        path().unfold()

// What were his ratings?

// This query store parts of the path and then show them with select step
// Start with user 710 again
g.V().has('user', 'id', 710).
//rating are stored in 'rated' edge property 'stars'
// find rating edges and remember them with 'as' modifier
        outE('rated').as('r').
// traverse to the movie
        inV().
// select Star Trek only and store them
        has('name', regex('Star Trek.*')).as('m').
// select 'name' from stored 'm' and 'stars' from 'r'
        select ('m', 'r').by('name').by('stars')

/**
 * OLAP Queries
*/

//Make an in memory snapshot, for faster spark operations. Use Spark cache
s = graph.snapshot().conf("gremlin.spark.persistStorageLevel", "MEMORY_ONLY").
        conf("gremlin.spark.graphStorageLevel", "MEMORY_ONLY").
        create()

// How many movies the dataset have for each genre?

// get all 'movie' vertices
s.V().hasLabel("movie").
// find genre
        out("genre").
// count by genre name
        groupCount().by("name")


// Who are watching Sci-Fi?

// that is a long run genre->movie->user->occupation
// start with Ski-Fi genre
s.V().has('genre', 'name', 'Sci-Fi').
// follow 'genre' edge to 'movies'
        in('genre').
// by 'rated' edge to users
        in('rated').
// by 'occupation' edge to 'occupation' vertex
        out('occupation').
// group the vertices by name and count each group
        groupCount().by('name')

// What are programmers watching

// let's go in the opposite direction from occupation to genre
s.V().has('occupation', 'name', 'programmer').
        in('occupation').
        out('rated').
        out('genre').
        groupCount().by('name')

// What is “Star Trek” raiting

// project step  with 'by' modifiers allows to get results from different  subqueries
s.V().has('movie', 'name', regex('Star Trek.*')).project("name", "rating").
// get name from the current movie vertex
        by(values('name')).
// rating from 'in' edges. Everege of all of them
        by(__.inE().values('stars').mean())

