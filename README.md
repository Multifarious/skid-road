# skid-road
----

## Purpose

Skid Road is a generic distributed collection system for streaming, line-oriented output.

Skid Road is named after the [historical Skid Road](http://www.nps.gov/nr/travel/seattle/s28.htm) in Seattle that was used to convey logs.

## Integration with a Dropwizard Service

TODO.

## Configuration

TODO.

## Access with Maven

### Coordinates

Include the following in your `pom.xml`:

<pre>
&lt;dependency>
  &lt;groupId>io.ifar&lt;/groupId>
  &lt;artifactId>skid-road&lt;/artifactId>
  &lt;version>0-SNAPSHOT&lt;/version>
&lt;/dependency>
</pre>

### Snapshots

Snapshots are available from the following Maven repository:

<pre>
&lt;repository>
  &lt;id>multifarious-snapshots&lt;/id>
  &lt;name>Multifarious, Inc. Snapshot Repository&lt;/name>
  &lt;url>http://repository-multifarious.forge.cloudbees.com/snapshot/&lt;/url>
  &lt;snapshots>
    &lt;enabled>true&lt;/enabled>
  &lt;/snapshots>
  &lt;releases>
    &lt;enabled>false&lt;/enabled>
  &lt;/releases>
&lt;/repository>
</pre>

### Releases

None as yet, but when there are, they will be published via Maven Central.

## License

The license is [BSD 2-clause](http://opensource.org/licenses/BSD-2-Clause).  This information is also present in the `LICENSE` file and in the `pom.xml`.
