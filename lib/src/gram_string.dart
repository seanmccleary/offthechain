import 'gram_item.dart';

/// A simple string container that compares case-insensitively.
///
/// If your markov chain contains strings, it'll probably give you better
/// results to use this.
class GramString extends GramItem {

  static final RegExp _makeComparableRegExp = new RegExp(r'[^A-Za-z0-9]');

  /// The string represented by this
  String innerString;

  String _comparableString;

  /// Instantiates a [GramString]
  GramString(this.innerString) {
    _comparableString = innerString.replaceAll(_makeComparableRegExp, "").toLowerCase();
  }


  @override
  String get comparableString => _comparableString;

  @override
  String toString() => innerString;
}