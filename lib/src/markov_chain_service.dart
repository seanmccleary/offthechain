import 'dart:async';
import 'package:offthechain/src/corpus.dart';
import 'package:offthechain/src/gram.dart';
import 'package:offthechain/src/gram_item.dart';
import 'package:offthechain/src/repositories/gram_repository.dart';
import 'package:logging/logging.dart' as log;

/// A service to manipulate a Markov chain
class MarkovChainService<T extends GramItem> {

  static final log.Logger _logger = new log.Logger('MarkovChainService');
  GramRepository<T> _gramRepo;

  /// Instantiates a [MarkovChainService]
  MarkovChainService(this._gramRepo);

  /// Adds a sequence of items to the corpus
  Future<dynamic> addSequence(List<T> words, Corpus corpus, [int gramLength = 3]) async {

    _logger.finer("Adding sequence: ${words.join(" ")}");

    final List<Gram<T>> gramsToSave = new List<Gram<T>>();

    for (int wordCount = 0; wordCount < words.length - (gramLength - 1); wordCount++) {

      final List<T> items = new List<T>();

      for (int itemCount = 0; itemCount < gramLength; itemCount++) {
        items.add(words[wordCount + itemCount]);
      }

      // OK now test if we've seen these texts on a gram before
      Gram<T> gram = await _gramRepo.getByItems(items, corpus.id);
      if (gram != null) {
        gram.occurrences++;
      } else {
        gram = new Gram<T>(corpus.id, 1);
        items.forEach((T item) => gram.addItem(item));
      }

      if (wordCount == 0) {
        gram.isStartOfChain = true;
      } else if(wordCount == words.length - (gramLength)) {
        gram.isEndOfChain = true;
      }
      gramsToSave.add(gram);
    }
    await _gramRepo.save(gramsToSave);
  }

  /// Gets a sequence from the corpus
  Future<List<T>> getSequence(List<String> _corpusIds) async {

    _logger.finer("Getting a sequence for corpus ${_corpusIds.join(", ")}");

    final List<T> items = new List<T>();
    Gram<T> gram = await _gramRepo.getRandomStartingGram(_corpusIds);
    if (gram == null || gram.items.isEmpty) {
      return null;
    }

    gram.items.where((T item) => item != null)
        .forEach(items.add);

    while (!gram.isEndOfChain) {
      gram = await _gramRepo.getByTextPrefix(gram.items.getRange(
          1, gram.items.length).toList(), _corpusIds);
      items.add(gram.items.last);
    }

    return items;
  }
}