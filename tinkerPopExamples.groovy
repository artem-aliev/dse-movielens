
// generate random subgraph with coin step. No comments for now. Could return later
g.V().has('genre', 'name', 'Sci-Fi').
        in('genre').coin(0.02).
        in('rated').coin(0.001).
        out('occupation').
        path().unfold()

/**
 * OLTP Vertex Centric Queries
*/

// What “Star Trek” movies did a user rate?

//Start at the vertex for user id 710. We have no names in the dataset
g.V().has('user', 'id', 710).
// Traverse to all connected vertices by 'rated' edge.
        out('rated').
// Filter out everything but "Star Trek" movies
        has('name', regex('Star Trek.*'))

// Let's display what was visited by the traversal
g.V().has('user', 'id', 710).
        out('rated').
        has('name', regex('Star Trek.*')).
// path() step gives us all visited paths,
// unfold() "flatMap" them into set of verticies
        path().unfold()

// Add genre and see as one more set of hops
g.V().has('user', 'id', 710).
        out('rated').
        has('name', regex('Star Trek.*')).
        out("genre").
        path().unfold()

// What were 710's ratings of Star Trek movies??

// This query store parts of the path and then shows them with select step
// Start with user 710 again
g.V().has('user', 'id', 710).
// Ratings are stored in 'rated' edge property 'stars'
// Find rating edges and remember them with 'as' modifier
        outE('rated').as('r').
// Traverse to the movie
        inV().
// Select Star Trek only and store them
        has('name', regex('Star Trek.*')).as('m').
// Select 'name' from stored 'm' and 'stars' from 'r'
        select ('m', 'r').by('name').by('stars')

/**
 * OLAP Queries
*/

//Make an in memory snapshot, for faster Spark operations. Use Spark cache
s = graph.snapshot().conf("gremlin.spark.persistStorageLevel", "MEMORY_ONLY").
        conf("gremlin.spark.graphStorageLevel", "MEMORY_ONLY").
        create()

// How many movies are in the dataset for each genre?

// Get all 'movie' vertices
s.V().hasLabel("movie").
// Find genre
        out("genre").
// Count by genre name
        groupCount().by("name")


// Who is watching Sci-Fi?

// This is a long traversal: genre->movie->user->occupation
// Start with Sci-Fi genre
s.V().has('genre', 'name', 'Sci-Fi').
// Follow 'genre' edge to 'movies'
        in('genre').
// By 'rated' edge to users
        in('rated').
// By 'occupation' edge to 'occupation' vertex
        out('occupation').
// Group the vertices by name and count each group
        groupCount().by('name')

// What genres are programmers watching?

// Let's go in the opposite direction from occupation to genre
s.V().has('occupation', 'name', 'programmer').
        in('occupation').
        out('rated').
        out('genre').
        groupCount().by('name')

// What is average “Star Trek” rating?

// Project step  with 'by' modifiers allows us to get results from different subqueries
s.V().has('movie', 'name', regex('Star Trek.*')).project("name", "rating").
// Get the name from the current movie vertex
        by(values('name')).
// Rating from 'in' edges. Average of all of them
        by(__.inE().values('stars').mean())

