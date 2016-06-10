# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = "fluent-plugin-gcloud-pubsub"
  gem.description = "Google Cloud Pub/Sub input/output plugin for Fluentd event collector"
  gem.license     = "MIT"
  gem.homepage    = "https://github.com/mdoi/fluent-plugin-gcloud-pubsub"
  gem.summary     = gem.description
  gem.version     = "0.0.4"
  gem.authors     = ["Masayuki DOI"]
  gem.email       = "dotquasar@gmail.com"
  gem.has_rdoc    = false
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_runtime_dependency "fluentd", "~> 0.12.0"
  gem.add_runtime_dependency "gcloud", "= 0.6.3"
  gem.add_runtime_dependency "fluent-plugin-buffer-lightening", ">= 0.0.2"

  gem.add_development_dependency "bundler"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "test-unit"
  gem.add_development_dependency "test-unit-rr"
end
