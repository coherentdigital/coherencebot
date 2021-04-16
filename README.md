CoherenceBot README
===================

CoherenceBot is a fork of Apache Nutch.  It was forked on 16 April 2021 corresponding to Apache Nutch 1.19.
Having a local fork allows us to commit changes rapidly (without review by the Nutch community)
and also allowing our modifications to be CoherenceBot-specific.

Custom Plugins in our fork include:

1. index-criteria - a plugin to remove unwanted docs from the downstream index.
2. index-org - a plugin which obtains organization metadata for including in the index.
3. parse-heading - a plugin for generating a title from PDF fonts on page 1.
4. parse-thumb - a plugin to generate a thumbnail using the thum.io web service.
5. parse-translate - a plugin that translates non-English titles using AWS Translate.
6. language-identifier - an existing plugin modifified to use AWS Comprehend to detect language.
7. text-summarizer - a plugin to generate a summary from parsed text.

Apache Nutch
============
<img src="https://nutch.apache.org/assets/img/nutch_logo_tm.png" align="right" width="300" />

For the latest information about Nutch, please visit our website at:

   https://nutch.apache.org/

and our wiki, at:

   https://cwiki.apache.org/confluence/display/NUTCH/Home

To get started using Nutch read Tutorial:

   https://cwiki.apache.org/confluence/display/NUTCH/NutchTutorial

Contributing
============
To contribute a patch, follow these instructions (note that installing
[Hub](https://hub.github.com/) is not strictly required, but is recommended).

```
0. Download and install hub.github.com
1. File JIRA issue for your fix at https://issues.apache.org/jira/projects/NUTCH/issues
- you will get issue id NUTCH-xxx where xxx is the issue ID.
2. git clone https://github.com/apache/nutch.git
3. cd nutch
4. git checkout -b NUTCH-xxx
5. edit files (please try and include a test case if possible)
6. git status (make sure it shows what files you expected to edit)
7. Make sure that your code complies with the [Nutch codeformatting template](https://raw.githubusercontent.com/apache/nutch/master/eclipse-codeformat.xml), which is basially two space indents
8. git add <files>
9. git commit -m “fix for NUTCH-xxx contributed by <your username>”
10. git fork
11. git push -u <your git username> NUTCH-xxx
12. git pull-request
```

IDE setup
=========

Generate Eclipse project files

```
ant eclipse
```

and follow the instructions in [Importing existing projects](https://help.eclipse.org/2019-06/topic/org.eclipse.platform.doc.user/tasks/tasks-importproject.htm).

IntelliJ IDEA users can also import Eclipse projects using the ["Eclipser" plugin](https://www.tutorialspoint.com/intellij_idea/intellij_idea_migrating_from_eclipse.htm)https://plugins.jetbrains.com/plugin/7153-eclipser), see also [Importing Eclipse Projects into IntelliJ IDEA](https://www.jetbrains.com/help/idea/migrating-from-eclipse-to-intellij-idea.html#migratingEclipseProject).


Export Control
==============
This distribution includes cryptographic software.  The country in which you 
currently reside may have restrictions on the import, possession, use, and/or 
re-export to another country, of encryption software.  BEFORE using any encryption 
software, please check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to see if this is 
permitted.  See <https://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has 
classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which 
includes information security software using or performing cryptographic functions with 
asymmetric algorithms.  The form and manner of this Apache Software Foundation 
distribution makes it eligible for export under the License Exception ENC Technology 
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, 
Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software:

Apache Nutch uses the PDFBox API in its parse-tika plugin for extracting textual content 
and metadata from encrypted PDF files. See https://pdfbox.apache.org/ for more 
details on PDFBox.
