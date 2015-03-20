require 'asciidoctor'
require 'erb'
require './src/main/ruby/readme.rb'

options = {:mkdirs => true, :safe => :unsafe, :attributes => ['linkcss', 'allow-uri-read']}

guard 'shell' do
  watch(/^src\/[A-Z-a-z][^#]*\.adoc$/) {|m|
    SpringCloud::Build.render_file('src/main/asciidoc/README.adoc', :to_file => './README.adoc')
    Asciidoctor.render_file('src/main/asciidoc/spring-cloud-bus.adoc', options.merge(:to_dir => 'target/generated-docs'))
  }
end
