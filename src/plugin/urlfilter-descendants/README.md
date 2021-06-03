urlfilter-descendants
======================
  This plugin allows certain urls to be exempted when the external links are configured to be ignored.
  It requires that the outlink is a descendant of the inlink

# How to enable ?
Add `urlfilter-descendants` value to `plugin.includes` property
```xml
<property>
  <name>plugin.includes</name>
  <value>protocol-http|urlfilter-(regex|descendants)...</value>
</property>
```

## Testing the Rules :

After enabling the plugin, run:
   
`bin/nutch plugin urlfilter-descendants  org.apache.nutch.urlfilter.descendants.ExemptionUrlFilter http://fromurl.here http://tourl.here`


This should print `true` for toUrls which are accepted by this filter.
