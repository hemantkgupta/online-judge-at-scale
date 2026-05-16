import java.util.Scanner;

/**
 * Reference Java solution for the Sum-of-Two-Numbers smoke problem.
 *
 * The Firecracker in-guest agent writes the contestant source to
 * Solution.java in its working dir, then runs:
 *   javac -d <workdir> Solution.java
 *   java  -cp <workdir> Solution < <stdin>
 * The class name MUST be `Solution` (case-sensitive) — that's the class
 * the agent's run argv invokes.
 */
public class Solution {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        long a = sc.nextLong();
        long b = sc.nextLong();
        System.out.println(a + b);
    }
}
