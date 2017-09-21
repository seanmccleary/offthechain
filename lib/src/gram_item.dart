/// A class to represent an item in a Gram sequence
abstract class GramItem {

  /// An easily-comparable string representation of this object
  /// e.g. if it's a GramString, it might be a string all in lower-case
  /// and with all punctuation removed, so that "THE!" and "The" will both
  /// become "the" and we can identify them as equal for our purposes.
  /// If if it's an integer, maybe just a string representation of the integer.
  String get comparableString;

  @override
  bool operator ==(Object other) => other is GramItem &&
      comparableString == other.comparableString;

  @override
  int get hashCode => comparableString.hashCode;

}