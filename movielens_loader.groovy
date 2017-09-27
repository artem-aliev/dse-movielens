config create_schema: true

occupations = [0:'other', 1:'academic/educator', 2:'artist', \
               3:'clerical/admin', 4:'college/grad student', 5:'customer service', \
               6:'doctor/health care', 7:'executive/managerial', 8:'farmer', \
               9:'homemaker', 10:'K-12 student', 11:'lawyer', 12:'programmer', \
               13:'retired', 14:'sales/marketing', 15:'scientist', 16:'self-employed', \
               17:'technician/engineer', 18:'tradesman/craftsman', 19:'unemployed', 20:'writer']

inputDir = './ml-1m/'
movies = File.text(inputDir + 'movies.dat')
             .delimiter("::")
             .header('id', 'name', 'genre')
movieInfo = movies.map {
  def name = it['name']
  def matcher = name =~ /(?<name>.*) \((?<year>\d{4})\)$/
  if (matcher.matches()) {
    it['name'] = matcher.group('name')
    it['year'] = matcher.group('year').toInteger()
  } else {
    it['name'] = name
    it['year'] = 0
  }
  it
}

movieToGenre = movieInfo.flatMap {
  def id = it['id']
  it['genre'].split('\\|').collect {
    it = ['id': id, 'name': it] 
  }
}

users = File.text(inputDir + 'users.dat')
            .delimiter('::')
            .header('id', 'gender', 'age', 'occupation', 'zipcode')

ratings = File.text(inputDir + 'ratings.dat')
              .delimiter('::')
              .header('user_id', 'movie_id', 'stars', 'timestamp')

// movie
load(movieInfo).asVertices {
  label 'movie'
  key id: 'id'

  ignore 'genre'
}

// user
load(users).asVertices {
  label 'user'
  key 'id'

  ignore 'occupation'
}

// occupation
load(Generator.of {['id': it, 'name': occupations[it]]}
              .count(occupations.size())).asVertices {
  label 'occupation'
  key 'id'
}

// genre
load(movieToGenre).asVertices {
  label 'genre'
  key 'name'

  ignore 'id'
}

// movie -(genre)-> genre
load(movieToGenre).asEdges {
  label 'genre'

  outV 'id', {
    label 'movie'
    key 'id'
  }
  inV 'name', {
    label 'genre'
    key 'name'
  }
}

// user -(rated)-> movie
load(ratings).asEdges {
  label 'rated'

  outV 'user_id', {
    label 'user'
    key user_id: 'id'
  }
  inV 'movie_id', {
    label 'movie'
    key movie_id: 'id'
  }
}

// user -(occupation)-> occupation
load(users).asEdges {
  label 'occupation'

  ignore 'gender'
  ignore 'age'
  ignore 'zipcode'

  outV 'id', {
    label 'user'
    key 'id'
  }
  inV 'occupation', {
    label 'occupation'
    key occupation: 'id'
  }
}
