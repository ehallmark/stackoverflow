package analysis.error_codes

import java.util.*


class UnorderedPair<T>(val first: T, val second: T) {
    override fun equals(other: Any?): Boolean {
        if(other == null) return false
        return if (other is UnorderedPair<*>) {
            (first == other.first && second == other.second) || (first == other.second && second == other.first)
        } else false
    }

    override fun hashCode(): Int {
        return Math.min(Objects.hash(first, second), Objects.hash(second, first))
    }
}

