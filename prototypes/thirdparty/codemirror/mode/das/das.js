CodeMirror.defineMode('das', function(config) {

  var words = {};
  function define(style, string) {
    var split = string.split(' ');
    for(var i = 0; i < split.length; i++) {
      words[split[i]] = style;
    }
  };

  // Atoms
  //define('atom', 'true false');

  // Keywords
  // if then do else elif while until for in esac fi fin fil done exit set unset export function
  

  define('keyword', 'last in between');

  // Commands
  define('builtin', 'grep unique sum count min max avg');

  function tokenBase(stream, state) {

    var sol = stream.sol();
    var ch = stream.next();
 

    // TODO: this may be more fancy|| ch === '`''=
    if (ch === '\'' || ch === '"') {
      state.tokens.unshift(tokenString(ch));
      return tokenize(stream, state);
    }

    if (ch === '=') {
      //TODO: in theory we may wish to extend DAS and allow ""

      stream.eat('=');
      stream.eat(' ');
      stream.eatWhile(/[^ "']/);
      return  'string';
    }

/*
    if (ch === '#') {
      if (sol && stream.eat('!')) {
        stream.skipToEnd();
        return 'meta'; // 'comment'?
      }
      stream.skipToEnd();
      return 'comment';
    }
    if (ch === '$') {
      state.tokens.unshift(tokenDollar);
      return tokenize(stream, state);
    }
*/

    // TODO: ch === '='

    if (ch === '+' ) {
      return 'operator';
    }
    if (ch === '.') {
      stream.eat('.');
      stream.eatWhile(/\w/);
      return 'attribute';
    }
    if (/\d/.test(ch)) {
      stream.eatWhile(/\d/);
      if(!/\w/.test(stream.peek())) {
        return 'number';
      }
    }
    stream.eatWhile(/\w/);
    var cur = stream.current();

    if (stream.peek() === '=' && /\w+/.test(cur)) return 'def';


    return words.hasOwnProperty(cur) ? words[cur] : null;
  }

  function tokenString(quote) {
    return function(stream, state) {
      var next, end = false, escaped = false;
      while ((next = stream.next()) != null) {
        if (next === quote && !escaped) {
          end = true;
          break;
        }
        if (next === '$' && !escaped && quote !== '\'') {
          escaped = true;
          stream.backUp(1);
          state.tokens.unshift(tokenDollar);
          break;
        }
        escaped = !escaped && next === '\\';
      }
      if (end || !escaped) {
        state.tokens.shift();
      }
      return (quote === '`' || quote === ')' ? 'quote' : 'string');
    };
  };

  var tokenDollar = function(stream, state) {
    if (state.tokens.length > 1) stream.eat('$');
    var ch = stream.next(), hungry = /\w/;
    if (ch === '{') hungry = /[^}]/;
    if (ch === '(') {
      state.tokens[0] = tokenString(')');
      return tokenize(stream, state);
    }
    if (!/\d/.test(ch)) {
      stream.eatWhile(hungry);
      stream.eat('}');
    }
    state.tokens.shift();
    return 'def';
  };

  function tokenize(stream, state) {
    return (state.tokens[0] || tokenBase) (stream, state);
  };

  return {
    startState: function() {return {tokens:[]};},
    token: function(stream, state) {
      if (stream.eatSpace()) return null;
      return tokenize(stream, state);
    }
  };
});
  
CodeMirror.defineMIME('text/x-sh-das', 'das');
